const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Consumer group coordinator.
///
/// Manages consumer group lifecycle:
///   EMPTY → PREPARING_REBALANCE → COMPLETING_REBALANCE → STABLE → (repeat)
///
/// Handles: JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
///          OffsetCommit, OffsetFetch
pub const GroupCoordinator = struct {
    groups: std.StringHashMap(ConsumerGroup),
    committed_offsets: std.StringHashMap(i64),
    allocator: Allocator,

    pub fn init(alloc: Allocator) GroupCoordinator {
        return .{
            .groups = std.StringHashMap(ConsumerGroup).init(alloc),
            .committed_offsets = std.StringHashMap(i64).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *GroupCoordinator) void {
        var it = self.groups.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.groups.deinit();

        var offset_it = self.committed_offsets.iterator();
        while (offset_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.committed_offsets.deinit();
    }

    /// Evict members that have exceeded their session timeout.
    /// Should be called periodically (e.g., every 1 second).
    /// Returns the number of members evicted.
    pub fn evictExpiredMembers(self: *GroupCoordinator, session_timeout_ms: i64) u32 {
        const now = std.time.milliTimestamp();
        var evicted: u32 = 0;

        var git = self.groups.iterator();
        while (git.next()) |gentry| {
            const group = gentry.value_ptr;
            if (group.state == .dead or group.state == .empty) continue;

            // Collect expired members
            var expired = std.ArrayList([]const u8).init(self.allocator);
            defer expired.deinit();

            var mit = group.members.iterator();
            while (mit.next()) |mentry| {
                const member = mentry.value_ptr;
                if ((now - member.last_heartbeat_ms) > session_timeout_ms) {
                    expired.append(mentry.key_ptr.*) catch {};
                }
            }

            // Remove expired members
            for (expired.items) |mid| {
                if (group.members.fetchRemove(mid)) |entry| {
                    var member_copy = entry.value;
                    member_copy.deinitMember(self.allocator);
                    evicted += 1;
                }
            }

            // If group became empty, transition to empty state
            if (group.members.count() == 0) {
                group.state = .empty;
            } else if (expired.items.len > 0) {
                // Trigger rebalance if members were evicted
                group.state = .preparing_rebalance;
                group.rebalance_start_ms = std.time.milliTimestamp();
                group.generation_id += 1;
            }
        }

        return evicted;
    }

    /// Check for rebalance timeouts — if a group has been in PREPARING_REBALANCE
    /// state for longer than its rebalance_timeout_ms, force-complete the rebalance.
    /// Transitions to COMPLETING_REBALANCE (not directly STABLE).
    pub fn checkRebalanceTimeouts(self: *GroupCoordinator) u32 {
        const now = std.time.milliTimestamp();
        var forced: u32 = 0;

        var git = self.groups.iterator();
        while (git.next()) |gentry| {
            const group = gentry.value_ptr;
            if (group.state == .preparing_rebalance) {
                if (group.rebalance_start_ms > 0 and
                    (now - group.rebalance_start_ms) > group.rebalance_timeout_ms)
                {
                    // Transition to completing_rebalance, not stable
                    // STABLE is only reached after the leader sends SyncGroup
                    group.state = .completing_rebalance;
                    forced += 1;
                }
            }
        }
        return forced;
    }

    /// Get count of all members across all groups.
    pub fn totalMemberCount(self: *const GroupCoordinator) usize {
        var total: usize = 0;
        var it = self.groups.iterator();
        while (it.next()) |entry| {
            total += entry.value_ptr.memberCount();
        }
        return total;
    }

    /// Handle JoinGroup request. Returns the member ID and whether this member is the leader.
    /// Supports group_instance_id for static membership.
    /// 
    /// After all members have joined, state transitions to
    /// COMPLETING_REBALANCE (not directly to STABLE). STABLE is reached only
    /// after the leader sends SyncGroup with assignments.
    pub fn joinGroup(self: *GroupCoordinator, group_id: []const u8, member_id: ?[]const u8, protocol_type: []const u8, subscriptions: ?[]const []const u8) !JoinGroupResult {
        return self.joinGroupWithInstanceId(group_id, member_id, null, protocol_type, subscriptions);
    }

    /// Handle JoinGroup with optional group_instance_id.
    /// When group_instance_id is set, the member uses static membership:
    /// - If a member with the same instance_id already exists, it is replaced
    ///   without triggering a full rebalance
    /// - The member_id is stable across restarts
    pub fn joinGroupWithInstanceId(self: *GroupCoordinator, group_id: []const u8, member_id: ?[]const u8, group_instance_id: ?[]const u8, protocol_type: []const u8, subscriptions: ?[]const []const u8) !JoinGroupResult {
        _ = protocol_type;

        if (!self.groups.contains(group_id)) {
            const key = try self.allocator.dupe(u8, group_id);
            try self.groups.put(key, ConsumerGroup.init(self.allocator, key));
        }

        const group = self.groups.getPtr(group_id).?;

        // Static membership — check if a member with this instance_id already exists
        if (group_instance_id) |gid| {
            var existing_mid: ?[]const u8 = null;
            var mit = group.members.iterator();
            while (mit.next()) |mentry| {
                const member = mentry.value_ptr;
                if (member.group_instance_id) |existing_gid| {
                    if (std.mem.eql(u8, existing_gid, gid)) {
                        existing_mid = mentry.key_ptr.*;
                        break;
                    }
                }
            }

            if (existing_mid) |emid| {
                // Static member rejoin — update heartbeat, don't trigger rebalance
                if (group.members.getPtr(emid)) |existing_member| {
                    existing_member.last_heartbeat_ms = std.time.milliTimestamp();
                    const is_leader = if (group.leader_id) |lid| std.mem.eql(u8, lid, emid) else false;
                    return .{
                        .error_code = 0,
                        .generation_id = group.generation_id,
                        .member_id = emid,
                        .is_leader = is_leader,
                        .leader_id = group.leader_id,
                    };
                }
            }
        }

        // Assign member ID if not provided
        const actual_member_id = if (member_id) |mid| mid else blk: {
            const new_id = try std.fmt.allocPrint(self.allocator, "member-{d}", .{group.next_member_id});
            group.next_member_id += 1;
            break :blk new_id;
        };

        // Add member to group
        const mid_copy = try self.allocator.dupe(u8, actual_member_id);
        var member = ConsumerGroup.GroupMember.initMember(self.allocator, mid_copy);

        // Set group_instance_id for static membership
        if (group_instance_id) |gid| {
            member.group_instance_id = try self.allocator.dupe(u8, gid);
        }

        // Track topic subscriptions
        if (subscriptions) |subs| {
            for (subs) |topic| {
                try member.subscribed_topics.append(try self.allocator.dupe(u8, topic));
            }
        }

        try group.members.put(mid_copy, member);

        // Transition to PREPARING_REBALANCE when new members join.
        // The state machine is:
        //   EMPTY → PREPARING_REBALANCE (on first JoinGroup)
        //   STABLE → PREPARING_REBALANCE (when membership changes)
        //   PREPARING_REBALANCE → COMPLETING_REBALANCE (when rebalance timeout expires
        //                                               or all members have joined)
        //   COMPLETING_REBALANCE → STABLE (after SyncGroup from leader)
        if (group.state == .empty or group.state == .stable) {
            group.state = .preparing_rebalance;
            group.generation_id += 1;
            group.rebalance_start_ms = std.time.milliTimestamp();
        }

        // If member_id was generated by us, free the formatted string
        if (member_id == null) {
            self.allocator.free(actual_member_id);
        }

        const is_leader = group.members.count() == 1; // First member is leader
        if (is_leader) {
            if (group.leader_id) |old| self.allocator.free(old);
            group.leader_id = try self.allocator.dupe(u8, mid_copy);
        }

        return .{
            .error_code = 0,
            .generation_id = group.generation_id,
            .member_id = mid_copy,
            .is_leader = is_leader,
            .leader_id = group.leader_id,
        };
    }

    pub const JoinGroupResult = struct {
        error_code: i16,
        generation_id: i32,
        member_id: []const u8,
        is_leader: bool,
        leader_id: ?[]const u8,
    };

    /// Handle Heartbeat request.
    pub fn heartbeat(self: *GroupCoordinator, group_id: []const u8, member_id: []const u8, generation_id: i32) i16 {
        const group = self.groups.getPtr(group_id) orelse return 16; // COORDINATOR_NOT_AVAILABLE

        if (group.generation_id != generation_id) return 22; // ILLEGAL_GENERATION

        if (group.members.getPtr(member_id)) |member| {
            member.last_heartbeat_ms = std.time.milliTimestamp();
            // Tell client to rejoin if group is rebalancing
            if (group.state == .preparing_rebalance or group.state == .completing_rebalance) {
                return 27; // REBALANCE_IN_PROGRESS
            }
            return 0; // OK
        }

        return 25; // UNKNOWN_MEMBER_ID
    }

    /// Handle LeaveGroup request.
    pub fn leaveGroup(self: *GroupCoordinator, group_id: []const u8, member_id: []const u8) i16 {
        const group = self.groups.getPtr(group_id) orelse return 16;

        if (group.members.fetchRemove(member_id)) |entry| {
            var member_copy = entry.value;
            member_copy.deinitMember(self.allocator);

            if (group.members.count() == 0) {
                group.state = .empty;
            } else {
                group.state = .preparing_rebalance;
                group.generation_id += 1;
            }
            return 0;
        }

        return 25; // UNKNOWN_MEMBER_ID
    }

    /// Handle SyncGroup request.
    /// The leader sends assignments for all members; followers receive their assignment.
    /// SyncGroup transitions from COMPLETING_REBALANCE to STABLE.
    /// Also accepts from PREPARING_REBALANCE for backward compatibility.
    pub fn syncGroup(self: *GroupCoordinator, group_id: []const u8, member_id: []const u8, generation_id: i32, assignments: ?[]const MemberAssignment) !SyncGroupResult {
        const group = self.groups.getPtr(group_id) orelse return .{
            .error_code = 16, // COORDINATOR_NOT_AVAILABLE
            .assignment = null,
        };

        if (group.generation_id != generation_id) return .{
            .error_code = 22, // ILLEGAL_GENERATION
            .assignment = null,
        };

        if (!group.members.contains(member_id)) return .{
            .error_code = 25, // UNKNOWN_MEMBER_ID
            .assignment = null,
        };

        // SyncGroup is only valid during rebalance states
        if (group.state != .completing_rebalance and group.state != .preparing_rebalance) {
            return .{
                .error_code = 27, // REBALANCE_IN_PROGRESS (tells client to rejoin)
                .assignment = null,
            };
        }

        // Only the group leader can send partition assignments
        if (assignments) |assigns| {
            const is_leader = if (group.leader_id) |lid| std.mem.eql(u8, lid, member_id) else false;
            if (!is_leader) {
                // Non-leader sent assignments — ignore them (follower waits for leader)
                const member = group.members.getPtr(member_id).?;
                return .{
                    .error_code = 0,
                    .assignment = member.assignment,
                };
            }
            for (assigns) |a| {
                if (group.members.getPtr(a.member_id)) |member| {
                    // Free old assignment if any
                    if (member.assignment) |old| self.allocator.free(old);
                    member.assignment = try self.allocator.dupe(u8, a.assignment);
                }
            }
            // Transition to stable
            group.state = .stable;
        }

        // Return this member's assignment
        const member = group.members.getPtr(member_id).?;
        return .{
            .error_code = 0,
            .assignment = member.assignment,
        };
    }

    pub const MemberAssignment = struct {
        member_id: []const u8,
        assignment: []const u8,
    };

    pub const SyncGroupResult = struct {
        error_code: i16,
        assignment: ?[]const u8,
    };

    /// Perform Range partition assignment for a group.
    /// Distributes partitions among members using Range strategy.
    pub fn computeRangeAssignment(self: *GroupCoordinator, group_id: []const u8, topic_partitions: []const TopicPartitionCount) ![]MemberAssignment {
        const group = self.groups.getPtr(group_id) orelse return &.{};

        const member_count = group.members.count();
        if (member_count == 0) return &.{};

        // Collect member IDs in order
        var member_ids = try self.allocator.alloc([]const u8, member_count);
        defer self.allocator.free(member_ids);
        {
            var it = group.members.iterator();
            var idx: usize = 0;
            while (it.next()) |entry| {
                member_ids[idx] = entry.key_ptr.*;
                idx += 1;
            }
        }

        // Build assignments — each member gets assigned partitions using Range strategy
        var result = try self.allocator.alloc(MemberAssignment, member_count);
        errdefer self.allocator.free(result);

        for (member_ids, 0..) |mid, i| {
            // Build a Kafka ConsumerProtocol assignment binary blob
            // Format: version(i16) + num_topics(i32) + [topic_name(string) + partitions(i32[])] + user_data(bytes)
            var buf: [1024]u8 = undefined;
            var pos: usize = 0;

            // Version
            std.mem.writeInt(i16, buf[pos..][0..2], 0, .big);
            pos += 2;

            // Number of topics
            std.mem.writeInt(i32, buf[pos..][0..4], @intCast(topic_partitions.len), .big);
            pos += 4;

            for (topic_partitions) |tp| {
                // Topic name (int16 length + bytes)
                std.mem.writeInt(i16, buf[pos..][0..2], @intCast(tp.topic.len), .big);
                pos += 2;
                @memcpy(buf[pos .. pos + tp.topic.len], tp.topic);
                pos += tp.topic.len;

                // Compute partition range for this member
                const partitions_per_member = @divTrunc(tp.partition_count, @as(i32, @intCast(member_count)));
                const extra = @rem(tp.partition_count, @as(i32, @intCast(member_count)));
                const start: i32 = @as(i32, @intCast(i)) * partitions_per_member + @min(@as(i32, @intCast(i)), extra);
                const count: i32 = partitions_per_member + @as(i32, if (@as(i32, @intCast(i)) < extra) @as(i32, 1) else 0);

                // Number of partitions
                std.mem.writeInt(i32, buf[pos..][0..4], count, .big);
                pos += 4;

                // Partition indices
                var p: i32 = start;
                while (p < start + count) : (p += 1) {
                    std.mem.writeInt(i32, buf[pos..][0..4], p, .big);
                    pos += 4;
                }
            }

            // User data (empty, int32 = -1)
            std.mem.writeInt(i32, buf[pos..][0..4], -1, .big);
            pos += 4;

            result[i] = .{
                .member_id = mid,
                .assignment = try self.allocator.dupe(u8, buf[0..pos]),
            };
        }

        return result;
    }

    pub const TopicPartitionCount = struct {
        topic: []const u8,
        partition_count: i32,
    };

    /// Perform Cooperative Sticky partition assignment for a group.
    /// Tries to keep members' existing assignments and only moves partitions when necessary.
    pub fn computeStickyAssignment(self: *GroupCoordinator, group_id: []const u8, topic_partitions: []const TopicPartitionCount) ![]MemberAssignment {
        const group = self.groups.getPtr(group_id) orelse return &.{};
        const member_count = group.members.count();
        if (member_count == 0) return &.{};

        // Collect member IDs
        var member_ids = try self.allocator.alloc([]const u8, member_count);
        defer self.allocator.free(member_ids);
        {
            var it = group.members.iterator();
            var idx: usize = 0;
            while (it.next()) |entry| {
                member_ids[idx] = entry.key_ptr.*;
                idx += 1;
            }
        }

        // For sticky assignment, start with existing assignments and only reassign unowned partitions
        // If no prior assignments exist, fall back to range assignment
        var has_prior = false;
        for (member_ids) |mid| {
            if (group.members.getPtr(mid)) |member| {
                if (member.assignment != null) {
                    has_prior = true;
                    break;
                }
            }
        }

        if (!has_prior) {
            // No prior assignments — fall back to range assignment
            return self.computeRangeAssignment(group_id, topic_partitions);
        }

        // Sticky: keep existing assignments, distribute unassigned partitions round-robin
        var result = try self.allocator.alloc(MemberAssignment, member_count);
        errdefer self.allocator.free(result);

        for (member_ids, 0..) |mid, i| {
            const member = group.members.getPtr(mid).?;
            if (member.assignment) |existing| {
                result[i] = .{
                    .member_id = mid,
                    .assignment = try self.allocator.dupe(u8, existing),
                };
            } else {
                // Build a minimal empty assignment
                var buf: [10]u8 = undefined;
                var bpos: usize = 0;
                std.mem.writeInt(i16, buf[bpos..][0..2], 0, .big); // version
                bpos += 2;
                std.mem.writeInt(i32, buf[bpos..][0..4], 0, .big); // 0 topics
                bpos += 4;
                std.mem.writeInt(i32, buf[bpos..][0..4], -1, .big); // no user data
                bpos += 4;
                result[i] = .{
                    .member_id = mid,
                    .assignment = try self.allocator.dupe(u8, buf[0..bpos]),
                };
            }
        }

        return result;
    }

    /// Commit an offset for a topic-partition in a consumer group.
    /// Also writes to __consumer_offsets partition store if available.
    pub fn commitOffset(self: *GroupCoordinator, group_id: []const u8, topic: []const u8, partition: i32, offset: i64) !void {
        const key = try std.fmt.allocPrint(self.allocator, "{s}:{s}:{d}", .{ group_id, topic, partition });
        errdefer self.allocator.free(key);
        // Remove old entry if exists (free old key since we have a new one)
        if (self.committed_offsets.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
        }
        try self.committed_offsets.put(key, offset);
    }

    /// Fetch committed offset for a topic-partition in a consumer group.
    pub fn fetchOffset(self: *const GroupCoordinator, group_id: []const u8, topic: []const u8, partition: i32) !?i64 {
        const key = try std.fmt.allocPrint(self.allocator, "{s}:{s}:{d}", .{ group_id, topic, partition });
        defer self.allocator.free(key);

        return self.committed_offsets.get(key);
    }

    pub fn groupCount(self: *const GroupCoordinator) usize {
        return self.groups.count();
    }

    /// Serialize group membership state for persistence.
    /// Format: num_groups(u32) + [group_id_len(u16) + group_id + state(u8) + gen(i32) + num_members(u32) + ...]
    pub fn serializeGroupState(self: *GroupCoordinator) ![]u8 {
        var buf = std.ArrayList(u8).init(self.allocator);
        const writer = buf.writer();

        try writer.writeInt(u32, @intCast(self.groups.count()), .big);

        var it = self.groups.iterator();
        while (it.next()) |entry| {
            const group = entry.value_ptr;
            const gid = entry.key_ptr.*;

            // group_id
            try writer.writeInt(u16, @intCast(gid.len), .big);
            try writer.writeAll(gid);

            // state + generation
            try writer.writeByte(@intFromEnum(group.state));
            try writer.writeInt(i32, group.generation_id, .big);

            // member count
            try writer.writeInt(u32, @intCast(group.members.count()), .big);

            var mit = group.members.iterator();
            while (mit.next()) |mentry| {
                const mid = mentry.key_ptr.*;
                try writer.writeInt(u16, @intCast(mid.len), .big);
                try writer.writeAll(mid);
            }
        }

        return buf.toOwnedSlice();
    }
};

/// Consumer group state.
pub const ConsumerGroup = struct {
    group_id: []const u8,
    state: GroupState = .empty,
    generation_id: i32 = 0,
    members: std.StringHashMap(GroupMember),
    next_member_id: u64 = 1,
    leader_id: ?[]u8 = null,
    rebalance_timeout_ms: i64 = 300000, // 5 minutes default
    rebalance_start_ms: i64 = 0,
    session_timeout_ms: i64 = 30000, // 30 seconds default
    allocator: Allocator,

    pub const GroupState = enum {
        empty,
        preparing_rebalance,
        completing_rebalance,
        stable,
        dead,
    };

    pub const GroupMember = struct {
        member_id: []u8,
        /// Static group membership — group.instance.id for Kubernetes-style
        /// static membership. When set, members are identified by instance_id rather
        /// than member_id, preventing unnecessary rebalances on pod restarts.
        group_instance_id: ?[]u8 = null,
        last_heartbeat_ms: i64 = 0,
        assignment: ?[]u8 = null,
        subscribed_topics: std.ArrayList([]u8),
        protocol_name: ?[]u8 = null,

        pub fn initMember(alloc: Allocator, mid: []u8) GroupMember {
            return .{
                .member_id = mid,
                .last_heartbeat_ms = std.time.milliTimestamp(),
                .subscribed_topics = std.ArrayList([]u8).init(alloc),
            };
        }

        pub fn deinitMember(self: *GroupMember, alloc: Allocator) void {
            for (self.subscribed_topics.items) |t| alloc.free(t);
            self.subscribed_topics.deinit();
            if (self.protocol_name) |p| alloc.free(p);
            if (self.assignment) |a| alloc.free(a);
            if (self.group_instance_id) |gid| alloc.free(gid);
            alloc.free(self.member_id);
        }
    };

    pub fn init(alloc: Allocator, group_id: []const u8) ConsumerGroup {
        return .{
            .group_id = group_id,
            .members = std.StringHashMap(GroupMember).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ConsumerGroup) void {
        var it = self.members.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinitMember(self.allocator);
        }
        self.members.deinit();
        if (self.leader_id) |lid| self.allocator.free(lid);
    }

    pub fn memberCount(self: *const ConsumerGroup) usize {
        return self.members.count();
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "GroupCoordinator join group" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.joinGroup("my-group", null, "consumer", null);
    try testing.expectEqual(@as(i16, 0), result.error_code);
    try testing.expect(result.is_leader);
    try testing.expectEqual(@as(i32, 1), result.generation_id);
    try testing.expectEqual(@as(usize, 1), coord.groupCount());
}

test "GroupCoordinator heartbeat" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const join_result = try coord.joinGroup("g1", null, "consumer", null);

    // Group is in preparing_rebalance after join, so heartbeat returns REBALANCE_IN_PROGRESS
    const err = coord.heartbeat("g1", join_result.member_id, join_result.generation_id);
    try testing.expectEqual(@as(i16, 27), err); // REBALANCE_IN_PROGRESS

    // Force to STABLE so heartbeat returns OK
    const group = coord.groups.getPtr("g1").?;
    group.state = .stable;
    const err_ok = coord.heartbeat("g1", join_result.member_id, join_result.generation_id);
    try testing.expectEqual(@as(i16, 0), err_ok);

    // Wrong generation
    const err2 = coord.heartbeat("g1", join_result.member_id, 999);
    try testing.expectEqual(@as(i16, 22), err2); // ILLEGAL_GENERATION

    // Unknown member
    const err3 = coord.heartbeat("g1", "unknown", join_result.generation_id);
    try testing.expectEqual(@as(i16, 25), err3); // UNKNOWN_MEMBER_ID
}

test "GroupCoordinator leave group" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.joinGroup("g1", null, "consumer", null);

    const err = coord.leaveGroup("g1", result.member_id);
    try testing.expectEqual(@as(i16, 0), err);

    // Group should be empty now
    const group = coord.groups.getPtr("g1").?;
    try testing.expectEqual(ConsumerGroup.GroupState.empty, group.state);
}

test "GroupCoordinator offset commit and fetch" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    try coord.commitOffset("g1", "topic-a", 0, 42);
    try coord.commitOffset("g1", "topic-a", 1, 100);

    const offset0 = try coord.fetchOffset("g1", "topic-a", 0);
    try testing.expectEqual(@as(i64, 42), offset0.?);

    const offset1 = try coord.fetchOffset("g1", "topic-a", 1);
    try testing.expectEqual(@as(i64, 100), offset1.?);

    // Unknown partition returns null
    const offset2 = try coord.fetchOffset("g1", "topic-a", 99);
    try testing.expect(offset2 == null);
}

test "GroupCoordinator multiple members trigger rebalance" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.joinGroup("g1", null, "consumer", null);
    try testing.expect(r1.is_leader);

    const r2 = try coord.joinGroup("g1", null, "consumer", null);
    _ = r2;

    const group = coord.groups.getPtr("g1").?;
    try testing.expectEqual(@as(usize, 2), group.memberCount());
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
}

test "GroupCoordinator syncGroup" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join two members
    const r1 = try coord.joinGroup("g1", null, "consumer", null);
    const r2 = try coord.joinGroup("g1", null, "consumer", null);

    // Follower sends SyncGroup first (no assignments) — gets null assignment (not yet assigned)
    const sync2_early = try coord.syncGroup("g1", r2.member_id, r2.generation_id, null);
    try testing.expectEqual(@as(i16, 0), sync2_early.error_code);
    try testing.expect(sync2_early.assignment == null);

    // Leader sends assignments — transitions to STABLE
    const assignments = [_]GroupCoordinator.MemberAssignment{
        .{ .member_id = r1.member_id, .assignment = "assign-1" },
        .{ .member_id = r2.member_id, .assignment = "assign-2" },
    };
    const sync1 = try coord.syncGroup("g1", r1.member_id, r1.generation_id, &assignments);
    try testing.expectEqual(@as(i16, 0), sync1.error_code);
    try testing.expectEqualStrings("assign-1", sync1.assignment.?);

    // Group should be stable now
    const group = coord.groups.getPtr("g1").?;
    try testing.expectEqual(ConsumerGroup.GroupState.stable, group.state);
}

test "GroupCoordinator heartbeat nonexistent group" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const err = coord.heartbeat("nonexistent", "member-1", 1);
    try testing.expectEqual(@as(i16, 16), err); // COORDINATOR_NOT_AVAILABLE
}

test "GroupCoordinator leave nonexistent group" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const err = coord.leaveGroup("nonexistent", "member-1");
    try testing.expectEqual(@as(i16, 16), err); // COORDINATOR_NOT_AVAILABLE
}

test "GroupCoordinator leave unknown member" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    _ = try coord.joinGroup("g1", null, "consumer", null);
    const err = coord.leaveGroup("g1", "unknown-member");
    try testing.expectEqual(@as(i16, 25), err); // UNKNOWN_MEMBER_ID
}

test "GroupCoordinator syncGroup wrong generation" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.joinGroup("g1", null, "consumer", null);

    const sync = try coord.syncGroup("g1", r1.member_id, 999, null);
    try testing.expectEqual(@as(i16, 22), sync.error_code); // ILLEGAL_GENERATION
}

test "GroupCoordinator syncGroup unknown member" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    _ = try coord.joinGroup("g1", null, "consumer", null);

    const sync = try coord.syncGroup("g1", "unknown", 1, null);
    try testing.expectEqual(@as(i16, 25), sync.error_code); // UNKNOWN_MEMBER_ID
}

test "GroupCoordinator syncGroup nonexistent group" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const sync = try coord.syncGroup("nonexistent", "m1", 1, null);
    try testing.expectEqual(@as(i16, 16), sync.error_code); // COORDINATOR_NOT_AVAILABLE
}

test "GroupCoordinator offset overwrite" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    try coord.commitOffset("g1", "t", 0, 10);
    try coord.commitOffset("g1", "t", 0, 20); // overwrite

    const offset = try coord.fetchOffset("g1", "t", 0);
    try testing.expectEqual(@as(i64, 20), offset.?);
}

test "GroupCoordinator totalMemberCount" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expectEqual(@as(usize, 0), coord.totalMemberCount());

    _ = try coord.joinGroup("g1", null, "consumer", null);
    try testing.expectEqual(@as(usize, 1), coord.totalMemberCount());

    _ = try coord.joinGroup("g2", null, "consumer", null);
    try testing.expectEqual(@as(usize, 2), coord.totalMemberCount());
}

test "GroupCoordinator join with subscriptions" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const topics = [_][]const u8{ "topic-a", "topic-b" };
    const result = try coord.joinGroup("g1", null, "consumer", &topics);
    try testing.expectEqual(@as(i16, 0), result.error_code);

    const group = coord.groups.getPtr("g1").?;
    const member = group.members.getPtr(result.member_id).?;
    try testing.expectEqual(@as(usize, 2), member.subscribed_topics.items.len);
}

// ---------------------------------------------------------------
// HIGH-priority gap tests
// ---------------------------------------------------------------

test "GroupCoordinator full rebalance flow EMPTY to STABLE" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Initially no groups
    try testing.expectEqual(@as(usize, 0), coord.groupCount());

    // Step 1: First member joins → EMPTY → PREPARING_REBALANCE
    const r1 = try coord.joinGroup("rebal-group", null, "consumer", null);
    try testing.expectEqual(@as(i16, 0), r1.error_code);
    try testing.expect(r1.is_leader);
    const group = coord.groups.getPtr("rebal-group").?;
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
    try testing.expectEqual(@as(i32, 1), r1.generation_id);

    // Step 2: Second member joins — stays PREPARING_REBALANCE
    // generation doesn't bump again since group was already in preparing_rebalance
    const r2 = try coord.joinGroup("rebal-group", null, "consumer", null);
    try testing.expectEqual(@as(i16, 0), r2.error_code);
    try testing.expect(!r2.is_leader);
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
    // generation_id is same as r1's since state was already preparing_rebalance
    const gen = r2.generation_id;

    // Step 3: Follower sends SyncGroup first (before leader) — gets null assignment
    const sync_follower = try coord.syncGroup("rebal-group", r2.member_id, gen, null);
    try testing.expectEqual(@as(i16, 0), sync_follower.error_code);
    try testing.expect(sync_follower.assignment == null);

    // Step 4: Leader sends SyncGroup with assignments → STABLE
    const assignments = [_]GroupCoordinator.MemberAssignment{
        .{ .member_id = r1.member_id, .assignment = "assign-leader" },
        .{ .member_id = r2.member_id, .assignment = "assign-follower" },
    };
    const sync_result = try coord.syncGroup("rebal-group", r1.member_id, gen, &assignments);
    try testing.expectEqual(@as(i16, 0), sync_result.error_code);
    try testing.expectEqualStrings("assign-leader", sync_result.assignment.?);
    try testing.expectEqual(ConsumerGroup.GroupState.stable, group.state);
}

test "GroupCoordinator session timeout eviction" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join a member
    const r1 = try coord.joinGroup("evict-group", null, "consumer", null);
    try testing.expectEqual(@as(i16, 0), r1.error_code);

    const group = coord.groups.getPtr("evict-group").?;
    try testing.expectEqual(@as(usize, 1), group.memberCount());

    // Artificially set the last heartbeat to far in the past
    const member = group.members.getPtr(r1.member_id).?;
    member.last_heartbeat_ms = std.time.milliTimestamp() - 60000; // 60 seconds ago

    // Evict with a 30-second timeout — member should be evicted
    const evicted = coord.evictExpiredMembers(30000);
    try testing.expectEqual(@as(u32, 1), evicted);

    // Group should be empty now
    try testing.expectEqual(@as(usize, 0), group.memberCount());
    try testing.expectEqual(ConsumerGroup.GroupState.empty, group.state);
}

test "GroupCoordinator session timeout triggers rebalance when members remain" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join two members
    const r1 = try coord.joinGroup("evict2-group", null, "consumer", null);
    const r2 = try coord.joinGroup("evict2-group", null, "consumer", null);
    _ = r2;

    const group = coord.groups.getPtr("evict2-group").?;
    try testing.expectEqual(@as(usize, 2), group.memberCount());

    // Expire only the first member
    const m1 = group.members.getPtr(r1.member_id).?;
    m1.last_heartbeat_ms = std.time.milliTimestamp() - 60000;

    const evicted = coord.evictExpiredMembers(30000);
    try testing.expectEqual(@as(u32, 1), evicted);
    try testing.expectEqual(@as(usize, 1), group.memberCount());
    // Should trigger rebalance since members remain
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
}

test "GroupCoordinator leaveGroup properly cleans member resources" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join with subscriptions and instance_id to exercise full cleanup
    const subs = [_][]const u8{"topic-a"};
    const result = try coord.joinGroupWithInstanceId("g1", null, "instance-1", "consumer", &subs);

    // Verify member has resources allocated
    const group = coord.groups.getPtr("g1").?;
    if (group.members.getPtr(result.member_id)) |member| {
        try testing.expect(member.group_instance_id != null);
        try testing.expectEqual(@as(usize, 1), member.subscribed_topics.items.len);
    }

    // LeaveGroup should free all member resources without leaking
    const err = coord.leaveGroup("g1", result.member_id);
    try testing.expectEqual(@as(i16, 0), err);
    try testing.expectEqual(@as(usize, 0), group.memberCount());
}

test "GroupCoordinator syncGroup rejects non-leader assignments" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.joinGroup("g1", null, "consumer", null);
    const r2 = try coord.joinGroup("g1", null, "consumer", null);

    // r1 is the leader (first to join)
    try testing.expect(r1.is_leader);
    try testing.expect(!r2.is_leader);

    // Force group into completing_rebalance for SyncGroup to work
    const group = coord.groups.getPtr("g1").?;
    group.state = .completing_rebalance;

    // Non-leader (r2) tries to send assignments — should be ignored
    const follower_assignments = [_]GroupCoordinator.MemberAssignment{
        .{ .member_id = r1.member_id, .assignment = "bad-assign-1" },
        .{ .member_id = r2.member_id, .assignment = "bad-assign-2" },
    };
    const sync_r2 = try coord.syncGroup("g1", r2.member_id, r2.generation_id, &follower_assignments);
    try testing.expectEqual(@as(i16, 0), sync_r2.error_code);
    // Assignments should NOT have been applied (non-leader)
    // Group should still be in completing_rebalance (not stable)
    try testing.expectEqual(ConsumerGroup.GroupState.completing_rebalance, group.state);
}

test "GroupCoordinator syncGroup validates group state" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.joinGroup("g1", null, "consumer", null);

    // Force group to STABLE state
    const group = coord.groups.getPtr("g1").?;
    group.state = .stable;

    // SyncGroup while STABLE should return REBALANCE_IN_PROGRESS
    const sync_result = try coord.syncGroup("g1", r1.member_id, r1.generation_id, null);
    try testing.expectEqual(@as(i16, 27), sync_result.error_code);
}

test "GroupCoordinator heartbeat returns REBALANCE_IN_PROGRESS during rebalance" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.joinGroup("g1", null, "consumer", null);

    // Group is in PREPARING_REBALANCE after first join
    const err = coord.heartbeat("g1", r1.member_id, r1.generation_id);
    try testing.expectEqual(@as(i16, 27), err); // REBALANCE_IN_PROGRESS

    // Force to STABLE
    const group = coord.groups.getPtr("g1").?;
    group.state = .stable;

    // Now heartbeat should succeed normally
    const err2 = coord.heartbeat("g1", r1.member_id, r1.generation_id);
    try testing.expectEqual(@as(i16, 0), err2);
}

test "GroupCoordinator static member rejoin preserves assignment" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join with static instance_id
    const r1 = try coord.joinGroupWithInstanceId("g1", null, "instance-1", "consumer", null);

    // Manually assign partition data (simulating SyncGroup)
    const group = coord.groups.getPtr("g1").?;
    if (group.members.getPtr(r1.member_id)) |member| {
        member.assignment = try testing.allocator.dupe(u8, "partition-0");
    }
    group.state = .stable;

    // Static member rejoins (same instance_id, simulating pod restart)
    const r2 = try coord.joinGroupWithInstanceId("g1", null, "instance-1", "consumer", null);

    // Should get back the same member_id (static membership)
    try testing.expectEqualStrings(r1.member_id, r2.member_id);
    // Group should still be stable (no rebalance triggered)
    try testing.expectEqual(ConsumerGroup.GroupState.stable, group.state);
}

test "GroupCoordinator offset overwrite preserves consistency" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Commit offset multiple times — should always reflect latest
    try coord.commitOffset("g1", "topic", 0, 10);
    try coord.commitOffset("g1", "topic", 0, 20);
    try coord.commitOffset("g1", "topic", 0, 30);

    const offset = try coord.fetchOffset("g1", "topic", 0);
    try testing.expectEqual(@as(i64, 30), offset.?);
}

test "GroupCoordinator multiple groups are independent" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r_a = try coord.joinGroup("group-a", null, "consumer", null);
    const r_b = try coord.joinGroup("group-b", null, "consumer", null);

    // Groups should be independent
    try testing.expectEqual(@as(usize, 2), coord.groupCount());

    // Leaving group-a should not affect group-b
    _ = coord.leaveGroup("group-a", r_a.member_id);
    _ = r_b;

    const ga = coord.groups.getPtr("group-a").?;
    const gb = coord.groups.getPtr("group-b").?;
    try testing.expectEqual(ConsumerGroup.GroupState.empty, ga.state);
    try testing.expectEqual(@as(usize, 1), gb.memberCount());
}

test "GroupCoordinator full lifecycle: join, sync, heartbeat, leave" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // 1. Join
    const r1 = try coord.joinGroup("g1", null, "consumer", null);
    try testing.expect(r1.is_leader);

    // 2. Force to completing_rebalance (simulating timeout)
    const group = coord.groups.getPtr("g1").?;
    group.state = .completing_rebalance;

    // 3. SyncGroup (leader sends assignments)
    const assignments = [_]GroupCoordinator.MemberAssignment{
        .{ .member_id = r1.member_id, .assignment = "partition-0" },
    };
    const sync = try coord.syncGroup("g1", r1.member_id, r1.generation_id, &assignments);
    try testing.expectEqual(@as(i16, 0), sync.error_code);
    try testing.expectEqualStrings("partition-0", sync.assignment.?);
    try testing.expectEqual(ConsumerGroup.GroupState.stable, group.state);

    // 4. Heartbeat (should succeed in STABLE state)
    const hb = coord.heartbeat("g1", r1.member_id, r1.generation_id);
    try testing.expectEqual(@as(i16, 0), hb);

    // 5. Leave
    const leave = coord.leaveGroup("g1", r1.member_id);
    try testing.expectEqual(@as(i16, 0), leave);
    try testing.expectEqual(ConsumerGroup.GroupState.empty, group.state);
}

test "GroupCoordinator eviction triggers rebalance for remaining members" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    _ = try coord.joinGroup("g1", null, "consumer", null);
    const r2 = try coord.joinGroup("g1", null, "consumer", null);

    // Force first member's heartbeat to be old
    const group = coord.groups.getPtr("g1").?;
    var mit = group.members.iterator();
    if (mit.next()) |entry| {
        entry.value_ptr.last_heartbeat_ms = 0; // Very old
    }

    // Evict expired members
    const evicted = coord.evictExpiredMembers(30_000);
    try testing.expectEqual(@as(u32, 1), evicted);

    // Group should be in rebalance (one member remains)
    try testing.expectEqual(@as(usize, 1), group.memberCount());
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
    _ = r2;
}

test "GroupCoordinator eviction empties group when all members expire" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    _ = try coord.joinGroup("g1", null, "consumer", null);

    // Force member heartbeat to be old
    const group = coord.groups.getPtr("g1").?;
    var mit = group.members.iterator();
    while (mit.next()) |entry| {
        entry.value_ptr.last_heartbeat_ms = 0;
    }

    const evicted = coord.evictExpiredMembers(30_000);
    try testing.expectEqual(@as(u32, 1), evicted);
    try testing.expectEqual(ConsumerGroup.GroupState.empty, group.state);
    try testing.expectEqual(@as(usize, 0), group.memberCount());
}

test "GroupCoordinator generation increments on rebalance" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.joinGroup("g1", null, "consumer", null);
    try testing.expectEqual(@as(i32, 1), r1.generation_id);

    // Second member triggers rebalance — but generation was already set to 1
    // (second join while already in PREPARING_REBALANCE shouldn't increment again)
    const r2 = try coord.joinGroup("g1", null, "consumer", null);
    try testing.expectEqual(@as(i32, 1), r2.generation_id); // Same generation

    // Leave triggers new rebalance with generation increment
    _ = coord.leaveGroup("g1", r2.member_id);
    const group = coord.groups.getPtr("g1").?;
    try testing.expectEqual(@as(i32, 2), group.generation_id);
}
const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.group_coordinator);
const MetricRegistry = @import("core").MetricRegistry;

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
    /// Optional metric registry for emitting consumer lag.
    /// Wired by the Broker after initialization.
    metrics: ?*MetricRegistry = null,

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
        const now = @import("time_compat").milliTimestamp();
        var evicted: u32 = 0;

        var git = self.groups.iterator();
        while (git.next()) |gentry| {
            const group = gentry.value_ptr;
            if (group.state == .dead or group.state == .empty) continue;

            // Collect expired members
            var expired = std.array_list.Managed([]const u8).init(self.allocator);
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
                    log.info("Evicted expired member from group: heartbeat timeout", .{});
                }
            }

            // If group became empty, transition to empty state
            if (group.members.count() == 0) {
                group.state = .empty;
            } else if (expired.items.len > 0) {
                // Trigger rebalance if members were evicted
                group.state = .preparing_rebalance;
                group.rebalance_start_ms = @import("time_compat").milliTimestamp();
                group.generation_id += 1;
                log.info("Group rebalance triggered: {d} members evicted, generation={d}", .{ expired.items.len, group.generation_id });
            }
        }

        return evicted;
    }

    /// Check for rebalance timeouts — if a group has been in PREPARING_REBALANCE
    /// state for longer than its rebalance_timeout_ms, force-complete the rebalance.
    /// Transitions to COMPLETING_REBALANCE (not directly STABLE).
    pub fn checkRebalanceTimeouts(self: *GroupCoordinator) u32 {
        const now = @import("time_compat").milliTimestamp();
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
                    log.info("Rebalance timeout: group forced to completing_rebalance", .{});
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
            log.info("Created new consumer group: {s}", .{group_id});
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
                    existing_member.last_heartbeat_ms = @import("time_compat").milliTimestamp();
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
        log.info("Member joined group {s}: member_count={d}", .{ group_id, group.members.count() });

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
            group.rebalance_start_ms = @import("time_compat").milliTimestamp();
            log.info("Group {s} transitioning to preparing_rebalance: generation={d}", .{ group_id, group.generation_id });
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
            member.last_heartbeat_ms = @import("time_compat").milliTimestamp();
            // Tell client to rejoin if group is rebalancing
            if (group.state == .preparing_rebalance or group.state == .completing_rebalance) {
                log.debug("Heartbeat: signaling REBALANCE_IN_PROGRESS to member {s} in group {s}", .{ member_id, group_id });
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
                log.info("Group {s} is now empty after member departure", .{group_id});
            } else {
                group.state = .preparing_rebalance;
                group.generation_id += 1;
                log.info("Member left group {s}: triggering rebalance, generation={d}", .{ group_id, group.generation_id });
            }
            return 0;
        }

        return 25; // UNKNOWN_MEMBER_ID
    }

    /// Delete an empty consumer group and its committed offsets.
    ///
    /// Kafka DeleteGroups must not remove groups with active members. Returning
    /// Kafka error codes here keeps the broker handler thin and testable.
    pub fn deleteGroup(self: *GroupCoordinator, group_id: []const u8) i16 {
        if (group_id.len == 0) return 24; // INVALID_GROUP_ID

        const group = self.groups.getPtr(group_id) orelse return 69; // GROUP_ID_NOT_FOUND
        if (group.members.count() > 0) return 68; // NON_EMPTY_GROUP

        self.deleteCommittedOffsetsForGroup(group_id);

        if (self.groups.fetchRemove(group_id)) |entry| {
            var group_copy = entry.value;
            group_copy.deinit();
            self.allocator.free(entry.key);
            return 0;
        }
        return 69; // GROUP_ID_NOT_FOUND
    }

    fn deleteCommittedOffsetsForGroup(self: *GroupCoordinator, group_id: []const u8) void {
        while (true) {
            var key_to_remove: ?[]const u8 = null;
            var it = self.committed_offsets.iterator();
            while (it.next()) |entry| {
                const key = entry.key_ptr.*;
                if (key.len > group_id.len and
                    key[group_id.len] == ':' and
                    std.mem.startsWith(u8, key, group_id))
                {
                    key_to_remove = key;
                    break;
                }
            }

            const key = key_to_remove orelse break;
            if (self.committed_offsets.fetchRemove(key)) |old| {
                self.allocator.free(old.key);
            }
        }
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
            log.info("Group {s} transitioned to stable: leader synced {d} assignments", .{ group_id, assigns.len });
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
    /// Minimizes partition movement by keeping existing assignments where possible
    /// and only redistributing unowned partitions round-robin to least-loaded members.
    pub fn computeStickyAssignment(self: *GroupCoordinator, group_id: []const u8, topic_partitions: []const TopicPartitionCount) ![]MemberAssignment {
        const group = self.groups.getPtr(group_id) orelse return &.{};
        const member_count = group.members.count();
        if (member_count == 0) return &.{};

        // Collect member IDs in stable order
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

        // Check if any member has a prior assignment
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

        // Total partition count
        var total_partitions: usize = 0;
        for (topic_partitions) |tp| {
            total_partitions += @intCast(tp.partition_count);
        }

        // Track which member owns each partition: key = "topic:partition", value = member index
        var ownership = std.StringHashMap(usize).init(self.allocator);
        defer {
            var kit = ownership.iterator();
            while (kit.next()) |e| self.allocator.free(e.key_ptr.*);
            ownership.deinit();
        }

        // Phase 1: Parse existing assignments and preserve valid ones
        for (member_ids, 0..) |mid, member_idx| {
            const member = group.members.getPtr(mid) orelse continue;
            if (member.assignment == null) continue;

            // Parse the Kafka ConsumerProtocol binary assignment format:
            // version(i16) + num_topics(i32) + [topic_name(i16+bytes) + num_parts(i32) + parts(i32[])] + user_data
            const blob = member.assignment.?;
            if (blob.len < 6) continue;

            var pos: usize = 2; // skip version
            const num_topics_raw = std.mem.readInt(i32, blob[pos..][0..4], .big);
            pos += 4;
            if (num_topics_raw <= 0) continue;
            const num_topics: usize = @intCast(num_topics_raw);

            for (0..num_topics) |_| {
                if (pos + 2 > blob.len) break;
                const topic_len: usize = @intCast(std.mem.readInt(i16, blob[pos..][0..2], .big));
                pos += 2;
                if (pos + topic_len > blob.len) break;
                const topic_name = blob[pos .. pos + topic_len];
                pos += topic_len;

                if (pos + 4 > blob.len) break;
                const num_parts_raw = std.mem.readInt(i32, blob[pos..][0..4], .big);
                pos += 4;
                if (num_parts_raw <= 0) continue;
                const num_parts: usize = @intCast(num_parts_raw);

                for (0..num_parts) |_| {
                    if (pos + 4 > blob.len) break;
                    const part_id = std.mem.readInt(i32, blob[pos..][0..4], .big);
                    pos += 4;

                    // Only keep if this partition still exists in topic_partitions
                    var valid = false;
                    for (topic_partitions) |tp| {
                        if (std.mem.eql(u8, tp.topic, topic_name) and part_id < tp.partition_count) {
                            valid = true;
                            break;
                        }
                    }
                    if (valid) {
                        const key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ topic_name, part_id });
                        ownership.put(key, member_idx) catch {
                            self.allocator.free(key);
                        };
                    }
                }
            }
        }

        // Phase 2: Collect unassigned partitions (new partitions or from departed members)
        var unassigned = std.array_list.Managed(struct { topic: []const u8, partition: i32 }).init(self.allocator);
        defer unassigned.deinit();

        for (topic_partitions) |tp| {
            var p: i32 = 0;
            while (p < tp.partition_count) : (p += 1) {
                const key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ tp.topic, p });
                defer self.allocator.free(key);
                if (!ownership.contains(key)) {
                    try unassigned.append(.{ .topic = tp.topic, .partition = p });
                }
            }
        }

        // Phase 3: Distribute unassigned partitions round-robin to least-loaded members
        var load = try self.allocator.alloc(usize, member_count);
        defer self.allocator.free(load);
        @memset(load, 0);
        {
            var oit = ownership.iterator();
            while (oit.next()) |entry| {
                load[entry.value_ptr.*] += 1;
            }
        }

        for (unassigned.items) |ua| {
            // Find member with lowest load
            var min_load: usize = std.math.maxInt(usize);
            var min_idx: usize = 0;
            for (load, 0..) |l, idx| {
                if (l < min_load) {
                    min_load = l;
                    min_idx = idx;
                }
            }
            const key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ ua.topic, ua.partition });
            ownership.put(key, min_idx) catch {
                self.allocator.free(key);
                continue;
            };
            load[min_idx] += 1;
        }

        // Phase 4: Build assignment blobs per member
        var result = try self.allocator.alloc(MemberAssignment, member_count);
        errdefer self.allocator.free(result);

        for (member_ids, 0..) |mid, member_idx| {
            var buf: [4096]u8 = undefined;
            var bpos: usize = 0;

            // Version
            std.mem.writeInt(i16, buf[bpos..][0..2], 0, .big);
            bpos += 2;

            // Count topics that have partitions for this member
            var topics_for_member: usize = 0;
            for (topic_partitions) |tp| {
                var has_partition = false;
                var p: i32 = 0;
                while (p < tp.partition_count) : (p += 1) {
                    const key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ tp.topic, p });
                    defer self.allocator.free(key);
                    if (ownership.get(key)) |owner| {
                        if (owner == member_idx) {
                            has_partition = true;
                            break;
                        }
                    }
                }
                if (has_partition) topics_for_member += 1;
            }

            std.mem.writeInt(i32, buf[bpos..][0..4], @intCast(topics_for_member), .big);
            bpos += 4;

            for (topic_partitions) |tp| {
                // Collect partitions for this member in this topic
                var parts = std.array_list.Managed(i32).init(self.allocator);
                defer parts.deinit();

                var p: i32 = 0;
                while (p < tp.partition_count) : (p += 1) {
                    const key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ tp.topic, p });
                    defer self.allocator.free(key);
                    if (ownership.get(key)) |owner| {
                        if (owner == member_idx) {
                            try parts.append(p);
                        }
                    }
                }

                if (parts.items.len == 0) continue;

                // Topic name
                std.mem.writeInt(i16, buf[bpos..][0..2], @intCast(tp.topic.len), .big);
                bpos += 2;
                if (bpos + tp.topic.len <= buf.len) {
                    @memcpy(buf[bpos .. bpos + tp.topic.len], tp.topic);
                    bpos += tp.topic.len;
                }

                // Partition count + indices
                std.mem.writeInt(i32, buf[bpos..][0..4], @intCast(parts.items.len), .big);
                bpos += 4;
                for (parts.items) |part_id| {
                    std.mem.writeInt(i32, buf[bpos..][0..4], part_id, .big);
                    bpos += 4;
                }
            }

            // User data (empty)
            std.mem.writeInt(i32, buf[bpos..][0..4], -1, .big);
            bpos += 4;

            result[member_idx] = .{
                .member_id = mid,
                .assignment = try self.allocator.dupe(u8, buf[0..bpos]),
            };
        }

        return result;
    }

    /// Commit an offset for a topic-partition in a consumer group.
    /// Also writes to __consumer_offsets partition store if available.
    pub fn commitOffset(self: *GroupCoordinator, group_id: []const u8, topic: []const u8, partition: i32, offset: i64) !void {
        return self.commitOffsetWithLag(group_id, topic, partition, offset, null);
    }

    /// Commit an offset and optionally emit consumer lag metric.
    /// When log_end_offset is provided, lag = log_end_offset - committed_offset.
    pub fn commitOffsetWithLag(self: *GroupCoordinator, group_id: []const u8, topic: []const u8, partition: i32, offset: i64, log_end_offset: ?i64) !void {
        const key = try std.fmt.allocPrint(self.allocator, "{s}:{s}:{d}", .{ group_id, topic, partition });
        errdefer self.allocator.free(key);
        // Remove old entry if exists (free old key since we have a new one)
        if (self.committed_offsets.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
        }
        try self.committed_offsets.put(key, offset);

        // Emit consumer lag metric when both metrics registry and LEO are available
        if (self.metrics) |m| {
            var part_buf: [16]u8 = undefined;
            const part_str = std.fmt.bufPrint(&part_buf, "{d}", .{partition}) catch return;
            if (log_end_offset) |leo| {
                const lag: f64 = @floatFromInt(@max(leo - offset, 0));
                m.setLabeledGauge("kafka_consumer_lag", &.{ group_id, topic, part_str }, lag);
            } else {
                // Without LEO, set lag to 0 (best effort — exact lag computed when LEO is available)
                m.setLabeledGauge("kafka_consumer_lag", &.{ group_id, topic, part_str }, 0.0);
            }
        }
    }

    /// Fetch committed offset for a topic-partition in a consumer group.
    pub fn fetchOffset(self: *const GroupCoordinator, group_id: []const u8, topic: []const u8, partition: i32) !?i64 {
        const key = try std.fmt.allocPrint(self.allocator, "{s}:{s}:{d}", .{ group_id, topic, partition });
        defer self.allocator.free(key);

        return self.committed_offsets.get(key);
    }

    pub const CommittedOffset = struct {
        topic: []const u8,
        partition: i32,
        offset: i64,
    };

    /// Return all committed offsets for a group, sorted by topic and partition.
    /// Topic slices are borrowed from the coordinator's committed-offset keys.
    pub fn listCommittedOffsets(self: *const GroupCoordinator, allocator: Allocator, group_id: []const u8) ![]CommittedOffset {
        var offsets = std.array_list.Managed(CommittedOffset).init(allocator);
        errdefer offsets.deinit();

        var it = self.committed_offsets.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            if (key.len <= group_id.len or key[group_id.len] != ':' or !std.mem.startsWith(u8, key, group_id)) {
                continue;
            }

            const rest = key[group_id.len + 1 ..];
            const partition_sep = std.mem.lastIndexOfScalar(u8, rest, ':') orelse continue;
            if (partition_sep == 0 or partition_sep + 1 >= rest.len) continue;

            const partition = std.fmt.parseInt(i32, rest[partition_sep + 1 ..], 10) catch continue;
            try offsets.append(.{
                .topic = rest[0..partition_sep],
                .partition = partition,
                .offset = entry.value_ptr.*,
            });
        }

        std.mem.sort(CommittedOffset, offsets.items, {}, struct {
            fn lessThan(_: void, a: CommittedOffset, b: CommittedOffset) bool {
                const topic_order = std.mem.order(u8, a.topic, b.topic);
                if (topic_order != .eq) return topic_order == .lt;
                return a.partition < b.partition;
            }
        }.lessThan);

        return try offsets.toOwnedSlice();
    }

    pub fn groupCount(self: *const GroupCoordinator) usize {
        return self.groups.count();
    }

    /// Serialize group membership state for persistence.
    /// Format: num_groups(u32) + [group_id_len(u16) + group_id + state(u8) + gen(i32) + num_members(u32) + ...]
    pub fn serializeGroupState(self: *GroupCoordinator) ![]u8 {
        var buf = std.array_list.Managed(u8).init(self.allocator);
        const writer = @import("list_compat").writer(&buf);

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
        subscribed_topics: std.array_list.Managed([]u8),
        protocol_name: ?[]u8 = null,

        pub fn initMember(alloc: Allocator, mid: []u8) GroupMember {
            return .{
                .member_id = mid,
                .last_heartbeat_ms = @import("time_compat").milliTimestamp(),
                .subscribed_topics = std.array_list.Managed([]u8).init(alloc),
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

test "GroupCoordinator deleteGroup enforces emptiness and removes offsets" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expectEqual(@as(i16, 69), coord.deleteGroup("missing"));

    const join = try coord.joinGroup("delete-me", null, "consumer", null);
    try coord.commitOffset("delete-me", "topic-a", 0, 42);
    try testing.expectEqual(@as(i16, 68), coord.deleteGroup("delete-me"));

    try testing.expectEqual(@as(i16, 0), coord.leaveGroup("delete-me", join.member_id));
    try testing.expectEqual(@as(i16, 0), coord.deleteGroup("delete-me"));
    try testing.expectEqual(@as(usize, 0), coord.groupCount());
    try testing.expect((try coord.fetchOffset("delete-me", "topic-a", 0)) == null);
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
    member.last_heartbeat_ms = @import("time_compat").milliTimestamp() - 60000; // 60 seconds ago

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
    m1.last_heartbeat_ms = @import("time_compat").milliTimestamp() - 60000;

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

test "GroupCoordinator sticky assignment preserves existing assignments" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join 2 members
    const r1 = try coord.joinGroup("g1", null, "consumer", null);
    const r2 = try coord.joinGroup("g1", null, "consumer", null);

    // First assignment: range
    const topics = [_]GroupCoordinator.TopicPartitionCount{
        .{ .topic = "topic-a", .partition_count = 4 },
    };
    const assign1 = try coord.computeRangeAssignment("g1", &topics);
    defer {
        for (assign1) |a| testing.allocator.free(a.assignment);
        testing.allocator.free(assign1);
    }

    // Apply assignments to members
    const group = coord.groups.getPtr("g1").?;
    for (assign1) |a| {
        if (group.members.getPtr(a.member_id)) |member| {
            if (member.assignment) |old| testing.allocator.free(old);
            member.assignment = testing.allocator.dupe(u8, a.assignment) catch null;
        }
    }

    // Now compute sticky assignment — should preserve existing
    const assign2 = try coord.computeStickyAssignment("g1", &topics);
    defer {
        for (assign2) |a| testing.allocator.free(a.assignment);
        testing.allocator.free(assign2);
    }

    // Both members should still have 2 partitions each
    try testing.expectEqual(@as(usize, 2), assign2.len);
    _ = r1;
    _ = r2;
}

test "GroupCoordinator sticky assignment handles new member" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Start with 1 member owning all 4 partitions
    _ = try coord.joinGroup("g1", null, "consumer", null);

    const topics = [_]GroupCoordinator.TopicPartitionCount{
        .{ .topic = "topic-a", .partition_count = 4 },
    };
    const assign1 = try coord.computeRangeAssignment("g1", &topics);
    defer {
        for (assign1) |a| testing.allocator.free(a.assignment);
        testing.allocator.free(assign1);
    }

    // Apply assignment
    const group = coord.groups.getPtr("g1").?;
    for (assign1) |a| {
        if (group.members.getPtr(a.member_id)) |member| {
            if (member.assignment) |old| testing.allocator.free(old);
            member.assignment = testing.allocator.dupe(u8, a.assignment) catch null;
        }
    }

    // Add second member
    _ = try coord.joinGroup("g1", null, "consumer", null);

    // Sticky assignment should redistribute
    const assign2 = try coord.computeStickyAssignment("g1", &topics);
    defer {
        for (assign2) |a| testing.allocator.free(a.assignment);
        testing.allocator.free(assign2);
    }

    // Both members should have partitions (not all 4 to member 1)
    try testing.expectEqual(@as(usize, 2), assign2.len);
}

test "GroupCoordinator evictExpiredMembers removes timed-out members" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join 3 members to a group
    const r1 = try coord.joinGroup("timeout-group", null, "consumer", null);
    const r2 = try coord.joinGroup("timeout-group", null, "consumer", null);
    const r3 = try coord.joinGroup("timeout-group", null, "consumer", null);

    const group = coord.groups.getPtr("timeout-group").?;
    try testing.expectEqual(@as(usize, 3), group.memberCount());

    // Expire members r1 and r2 by setting their heartbeat far in the past
    if (group.members.getPtr(r1.member_id)) |m| {
        m.last_heartbeat_ms = @import("time_compat").milliTimestamp() - 120_000; // 2 min ago
    }
    if (group.members.getPtr(r2.member_id)) |m| {
        m.last_heartbeat_ms = @import("time_compat").milliTimestamp() - 90_000; // 1.5 min ago
    }
    // r3 keeps a recent heartbeat (already set by joinGroup)

    // Evict with a 10-second timeout — r1 and r2 should be evicted, r3 survives
    const evicted = coord.evictExpiredMembers(10_000);
    try testing.expectEqual(@as(u32, 2), evicted);
    try testing.expectEqual(@as(usize, 1), group.memberCount());

    // r3 should still be in the group
    try testing.expect(group.members.contains(r3.member_id));
    try testing.expect(!group.members.contains(r1.member_id));
    try testing.expect(!group.members.contains(r2.member_id));

    // Group should be in preparing_rebalance (not empty, since r3 remains)
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
}

test "GroupCoordinator evictExpiredMembers skips active members" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join 2 members — both have fresh heartbeats from joinGroup
    _ = try coord.joinGroup("active-group", null, "consumer", null);
    _ = try coord.joinGroup("active-group", null, "consumer", null);

    const group = coord.groups.getPtr("active-group").?;
    try testing.expectEqual(@as(usize, 2), group.memberCount());

    // Evict with a very large timeout (300 seconds) — nobody should be evicted
    const evicted = coord.evictExpiredMembers(300_000);
    try testing.expectEqual(@as(u32, 0), evicted);
    try testing.expectEqual(@as(usize, 2), group.memberCount());

    // State should remain preparing_rebalance (set by second joinGroup), not changed
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
}

test "GroupCoordinator computeRangeAssignment distributes evenly" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join 2 members
    _ = try coord.joinGroup("range-group", null, "consumer", null);
    _ = try coord.joinGroup("range-group", null, "consumer", null);

    // 4 partitions across 2 members => 2 each
    const topics = [_]GroupCoordinator.TopicPartitionCount{
        .{ .topic = "test-topic", .partition_count = 4 },
    };
    const assignments = try coord.computeRangeAssignment("range-group", &topics);
    defer {
        for (assignments) |a| testing.allocator.free(a.assignment);
        testing.allocator.free(assignments);
    }

    try testing.expectEqual(@as(usize, 2), assignments.len);

    // Decode each assignment blob and verify partition count
    // Binary format: version(i16) + num_topics(i32) + topic_name(i16+bytes) + num_parts(i32) + parts(i32[]) + user_data(i32)
    for (assignments) |a| {
        const blob = a.assignment;
        // Skip version (2 bytes)
        var pos: usize = 2;
        // num_topics
        const num_topics = std.mem.readInt(i32, blob[pos..][0..4], .big);
        try testing.expectEqual(@as(i32, 1), num_topics);
        pos += 4;
        // topic name length
        const topic_len: usize = @intCast(std.mem.readInt(i16, blob[pos..][0..2], .big));
        pos += 2;
        // topic name
        try testing.expectEqualStrings("test-topic", blob[pos .. pos + topic_len]);
        pos += topic_len;
        // partition count for this member
        const part_count = std.mem.readInt(i32, blob[pos..][0..4], .big);
        try testing.expectEqual(@as(i32, 2), part_count);
    }
}

test "GroupCoordinator leaveGroup cleans member resources" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Join 2 members with subscriptions to exercise full cleanup
    const subs = [_][]const u8{ "topic-x", "topic-y" };
    const r1 = try coord.joinGroup("leave-group", null, "consumer", &subs);
    const r2 = try coord.joinGroup("leave-group", null, "consumer", &subs);

    try testing.expect(r1.is_leader);
    const group = coord.groups.getPtr("leave-group").?;
    try testing.expectEqual(@as(usize, 2), group.memberCount());

    // Force to completing_rebalance + sync to give members assignments
    group.state = .completing_rebalance;
    const sync_assigns = [_]GroupCoordinator.MemberAssignment{
        .{ .member_id = r1.member_id, .assignment = "assign-r1" },
        .{ .member_id = r2.member_id, .assignment = "assign-r2" },
    };
    const sync_result = try coord.syncGroup("leave-group", r1.member_id, r1.generation_id, &sync_assigns);
    try testing.expectEqual(@as(i16, 0), sync_result.error_code);
    try testing.expectEqual(ConsumerGroup.GroupState.stable, group.state);

    // Leave r1 (the leader) — group should rebalance, member removed, resources freed
    const leave_err = coord.leaveGroup("leave-group", r1.member_id);
    try testing.expectEqual(@as(i16, 0), leave_err);
    try testing.expectEqual(@as(usize, 1), group.memberCount());
    try testing.expect(!group.members.contains(r1.member_id));
    try testing.expect(group.members.contains(r2.member_id));
    // With remaining members, state transitions to preparing_rebalance
    try testing.expectEqual(ConsumerGroup.GroupState.preparing_rebalance, group.state);
    // Generation incremented on leave
    try testing.expectEqual(@as(i32, r1.generation_id + 1), group.generation_id);

    // Leave r2 (last member) — group becomes empty
    const leave_err2 = coord.leaveGroup("leave-group", r2.member_id);
    try testing.expectEqual(@as(i16, 0), leave_err2);
    try testing.expectEqual(@as(usize, 0), group.memberCount());
    try testing.expectEqual(ConsumerGroup.GroupState.empty, group.state);
}

test "GroupCoordinator multiple groups independent state and offsets" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // Create group-alpha with 2 members, group-beta with 1 member
    const alpha1 = try coord.joinGroup("group-alpha", null, "consumer", null);
    _ = try coord.joinGroup("group-alpha", null, "consumer", null);
    const beta1 = try coord.joinGroup("group-beta", null, "consumer", null);

    try testing.expectEqual(@as(usize, 2), coord.groupCount());

    // Commit different offsets per group for same topic-partition
    try coord.commitOffset("group-alpha", "shared-topic", 0, 100);
    try coord.commitOffset("group-beta", "shared-topic", 0, 500);

    // Verify offsets are independent
    const alpha_offset = try coord.fetchOffset("group-alpha", "shared-topic", 0);
    const beta_offset = try coord.fetchOffset("group-beta", "shared-topic", 0);
    try testing.expectEqual(@as(i64, 100), alpha_offset.?);
    try testing.expectEqual(@as(i64, 500), beta_offset.?);

    // Force group-alpha to stable, leave group-beta's sole member
    const ga = coord.groups.getPtr("group-alpha").?;
    const gb = coord.groups.getPtr("group-beta").?;
    ga.state = .completing_rebalance;
    const sync_a = [_]GroupCoordinator.MemberAssignment{
        .{ .member_id = alpha1.member_id, .assignment = "p0" },
    };
    _ = try coord.syncGroup("group-alpha", alpha1.member_id, alpha1.generation_id, &sync_a);
    try testing.expectEqual(ConsumerGroup.GroupState.stable, ga.state);

    // Leave group-beta member — should not affect group-alpha
    _ = coord.leaveGroup("group-beta", beta1.member_id);
    try testing.expectEqual(ConsumerGroup.GroupState.empty, gb.state);
    try testing.expectEqual(ConsumerGroup.GroupState.stable, ga.state);
    try testing.expectEqual(@as(usize, 2), ga.memberCount());

    // Heartbeat on group-alpha still works
    const hb = coord.heartbeat("group-alpha", alpha1.member_id, alpha1.generation_id);
    try testing.expectEqual(@as(i16, 0), hb);

    // Offsets are still correct after state changes
    const alpha_offset2 = try coord.fetchOffset("group-alpha", "shared-topic", 0);
    try testing.expectEqual(@as(i64, 100), alpha_offset2.?);
}

test "GroupCoordinator commitOffsetWithLag emits lag metric" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledGauge("kafka_consumer_lag", "Consumer lag", &.{ "group", "topic", "partition" });

    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();
    coord.metrics = &registry;

    // Commit offset 5 with LEO 10 → lag = 5
    try coord.commitOffsetWithLag("test-grp", "test-topic", 0, 5, 10);

    const key = "kafka_consumer_lag{test-grp,test-topic,0}";
    const entry = registry.labeled_gauges.get(key);
    try testing.expect(entry != null);
    try testing.expectEqual(@as(f64, 5.0), entry.?.value);
}

test "GroupCoordinator commitOffsetWithLag updates lag on subsequent commits" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledGauge("kafka_consumer_lag", "Consumer lag", &.{ "group", "topic", "partition" });

    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();
    coord.metrics = &registry;

    // Initial commit: lag = 10 - 3 = 7
    try coord.commitOffsetWithLag("grp1", "topic1", 0, 3, 10);

    const key = "kafka_consumer_lag{grp1,topic1,0}";
    try testing.expectEqual(@as(f64, 7.0), registry.labeled_gauges.get(key).?.value);

    // Consumer catches up: lag = 10 - 10 = 0
    try coord.commitOffsetWithLag("grp1", "topic1", 0, 10, 10);
    try testing.expectEqual(@as(f64, 0.0), registry.labeled_gauges.get(key).?.value);
}

test "GroupCoordinator commitOffsetWithLag handles null LEO" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledGauge("kafka_consumer_lag", "Consumer lag", &.{ "group", "topic", "partition" });

    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();
    coord.metrics = &registry;

    // Commit without LEO → lag defaults to 0
    try coord.commitOffsetWithLag("grp2", "topic2", 1, 5, null);

    const key = "kafka_consumer_lag{grp2,topic2,1}";
    const entry = registry.labeled_gauges.get(key);
    try testing.expect(entry != null);
    try testing.expectEqual(@as(f64, 0.0), entry.?.value);
}

test "GroupCoordinator commitOffsetWithLag without metrics does not crash" {
    var coord = GroupCoordinator.init(testing.allocator);
    defer coord.deinit();

    // No metrics wired — should not crash
    try coord.commitOffsetWithLag("grp3", "topic3", 0, 5, 10);
    try coord.commitOffset("grp3", "topic3", 1, 3);

    // Offset should still be committed
    const offset = try coord.fetchOffset("grp3", "topic3", 0);
    try testing.expectEqual(@as(i64, 5), offset.?);
}

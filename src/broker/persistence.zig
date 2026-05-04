const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = @import("fs_compat");
const log = std.log.scoped(.persistence);
const Authorizer = @import("security").auth.Authorizer;

fn hasComptimeField(comptime T: type, comptime field_name: []const u8) bool {
    return switch (@typeInfo(T)) {
        .@"struct" => @hasField(T, field_name),
        else => false,
    };
}

/// Persists broker metadata (topics, offsets) to a JSON file in the data directory.
/// Loaded on startup, saved on changes.
pub const MetadataPersistence = struct {
    data_dir: ?[]const u8,
    allocator: Allocator,

    const default_topic_retention_ms: i64 = 604800000;
    const default_topic_retention_bytes: i64 = -1;
    const default_topic_max_message_bytes: i32 = 1048576;
    const default_topic_min_insync_replicas: i32 = 1;
    const default_topic_segment_bytes: i64 = 1073741824;
    const default_topic_cleanup_policy: []const u8 = "delete";
    const default_topic_compression_type: []const u8 = "producer";

    pub fn init(alloc: Allocator, data_dir: ?[]const u8) MetadataPersistence {
        return .{ .data_dir = data_dir, .allocator = alloc };
    }

    pub const AutoMqKvEntry = struct {
        key: []u8,
        value: []u8,
    };

    pub const AutoMqNodeEntry = struct {
        node_id: i32,
        node_epoch: i64,
        wal_config: []u8,
    };

    pub const AutoMqGroupPromotionEntry = struct {
        group_id: []u8,
        link_id: []u8,
        promoted: bool,
    };

    pub const AutoMqMetadataSnapshot = struct {
        next_node_id: i32,
        zone_router_epoch: i64,
        license: ?[]u8,
        zone_router_metadata: ?[]u8,
        kvs: []AutoMqKvEntry,
        nodes: []AutoMqNodeEntry,
        group_promotions: []AutoMqGroupPromotionEntry,
    };

    fn emptyAutoMqMetadataSnapshot() AutoMqMetadataSnapshot {
        return .{
            .next_node_id = 1,
            .zone_router_epoch = 0,
            .license = null,
            .zone_router_metadata = null,
            .kvs = &.{},
            .nodes = &.{},
            .group_promotions = &.{},
        };
    }

    fn writeHex(file: fs.File, bytes: []const u8) !void {
        const alphabet = "0123456789abcdef";
        var pair: [2]u8 = undefined;
        for (bytes) |byte| {
            pair[0] = alphabet[byte >> 4];
            pair[1] = alphabet[byte & 0x0f];
            try file.writeAll(&pair);
        }
    }

    fn decodeHexNibble(byte: u8) ?u8 {
        return switch (byte) {
            '0'...'9' => byte - '0',
            'a'...'f' => byte - 'a' + 10,
            'A'...'F' => byte - 'A' + 10,
            else => null,
        };
    }

    fn decodeHexAlloc(allocator: Allocator, text: []const u8) ![]u8 {
        if (text.len % 2 != 0) return error.InvalidHex;
        const out = try allocator.alloc(u8, text.len / 2);
        errdefer allocator.free(out);

        var i: usize = 0;
        while (i < out.len) : (i += 1) {
            const high = decodeHexNibble(text[i * 2]) orelse return error.InvalidHex;
            const low = decodeHexNibble(text[i * 2 + 1]) orelse return error.InvalidHex;
            out[i] = (high << 4) | low;
        }
        return out;
    }

    fn decodeUuidHex(text: []const u8) ![16]u8 {
        if (text.len != 32) return error.InvalidHex;

        var out: [16]u8 = undefined;
        for (&out, 0..) |*byte, i| {
            const high = decodeHexNibble(text[i * 2]) orelse return error.InvalidHex;
            const low = decodeHexNibble(text[i * 2 + 1]) orelse return error.InvalidHex;
            byte.* = (high << 4) | low;
        }
        return out;
    }

    fn writeI32Csv(file: fs.File, values: []const i32) !void {
        const writer = file.writer();
        for (values, 0..) |value, i| {
            if (i > 0) try file.writeAll(",");
            try writer.print("{d}", .{value});
        }
    }

    fn parseI32CsvAlloc(allocator: Allocator, text: []const u8) ![]i32 {
        if (text.len == 0) return &.{};

        var values = std.array_list.Managed(i32).init(allocator);
        defer values.deinit();

        var fields = std.mem.splitSequence(u8, text, ",");
        while (fields.next()) |field| {
            if (field.len == 0) return error.InvalidCsv;
            try values.append(try std.fmt.parseInt(i32, field, 10));
        }
        return values.toOwnedSlice();
    }

    fn parseBoolFlag(text: []const u8) !bool {
        if (std.mem.eql(u8, text, "0")) return false;
        if (std.mem.eql(u8, text, "1")) return true;
        return error.InvalidFlag;
    }

    fn decodeOptionalHexAlloc(allocator: Allocator, has_value_text: []const u8, hex_text: []const u8) !?[]u8 {
        if (!try parseBoolFlag(has_value_text)) return null;
        return try decodeHexAlloc(allocator, hex_text);
    }

    fn normalizeTopicCleanupPolicy(text: []const u8) ?[]const u8 {
        if (std.mem.eql(u8, text, "delete")) return "delete";
        if (std.mem.eql(u8, text, "compact")) return "compact";
        if (std.mem.eql(u8, text, "compact,delete")) return "compact,delete";
        if (std.mem.eql(u8, text, "delete,compact")) return "compact,delete";
        return null;
    }

    fn normalizeTopicCompressionType(text: []const u8) ?[]const u8 {
        if (std.mem.eql(u8, text, "producer")) return "producer";
        if (std.mem.eql(u8, text, "uncompressed")) return "uncompressed";
        if (std.mem.eql(u8, text, "gzip")) return "gzip";
        if (std.mem.eql(u8, text, "snappy")) return "snappy";
        if (std.mem.eql(u8, text, "lz4")) return "lz4";
        if (std.mem.eql(u8, text, "zstd")) return "zstd";
        return null;
    }

    fn freeAutoMqKvEntries(allocator: Allocator, entries: []AutoMqKvEntry) void {
        for (entries) |entry| {
            allocator.free(entry.key);
            allocator.free(entry.value);
        }
        if (entries.len > 0) allocator.free(entries);
    }

    fn freeAutoMqNodeEntries(allocator: Allocator, entries: []AutoMqNodeEntry) void {
        for (entries) |entry| {
            allocator.free(entry.wal_config);
        }
        if (entries.len > 0) allocator.free(entries);
    }

    fn freeAutoMqGroupPromotionEntries(allocator: Allocator, entries: []AutoMqGroupPromotionEntry) void {
        for (entries) |entry| {
            allocator.free(entry.group_id);
            allocator.free(entry.link_id);
        }
        if (entries.len > 0) allocator.free(entries);
    }

    pub fn freeAutoMqMetadataSnapshot(self: *MetadataPersistence, snapshot: *AutoMqMetadataSnapshot) void {
        if (snapshot.license) |license| self.allocator.free(license);
        if (snapshot.zone_router_metadata) |metadata| self.allocator.free(metadata);
        freeAutoMqKvEntries(self.allocator, snapshot.kvs);
        freeAutoMqNodeEntries(self.allocator, snapshot.nodes);
        freeAutoMqGroupPromotionEntries(self.allocator, snapshot.group_promotions);
        snapshot.* = emptyAutoMqMetadataSnapshot();
    }

    /// Save topic metadata to disk.
    /// Format: topic_v4 TSV with hex-encoded names, topic IDs, supported
    /// numeric configs, and normalized string/list configs. Legacy readers only
    /// understood raw `name\tpartitions\trf`; loadTopics keeps accepting that
    /// plus topic_v2/topic_v3 for rolling upgrades.
    pub fn saveTopics(self: *MetadataPersistence, topics: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/topics.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        const writer = file.writer();
        var it = topics.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr;
            const has_config = comptime hasComptimeField(@TypeOf(info.*), "config");
            const retention_ms = if (has_config) info.config.retention_ms else default_topic_retention_ms;
            const retention_bytes = if (has_config) info.config.retention_bytes else default_topic_retention_bytes;
            const max_message_bytes = if (has_config) info.config.max_message_bytes else default_topic_max_message_bytes;
            const min_insync_replicas = if (has_config) info.config.min_insync_replicas else default_topic_min_insync_replicas;
            const has_segment_bytes = if (has_config) comptime hasComptimeField(@TypeOf(info.config), "segment_bytes") else false;
            const has_cleanup_policy = if (has_config) comptime hasComptimeField(@TypeOf(info.config), "cleanup_policy") else false;
            const has_compression_type = if (has_config) comptime hasComptimeField(@TypeOf(info.config), "compression_type") else false;
            const segment_bytes = if (has_segment_bytes) info.config.segment_bytes else default_topic_segment_bytes;
            const cleanup_policy = if (has_cleanup_policy) info.config.cleanup_policy else default_topic_cleanup_policy;
            const compression_type = if (has_compression_type) info.config.compression_type else default_topic_compression_type;
            const has_topic_id = comptime hasComptimeField(@TypeOf(info.*), "topic_id");
            const topic_id: [16]u8 = if (has_topic_id) info.topic_id else [_]u8{0} ** 16;

            try file.writeAll("topic_v4\t");
            try writeHex(file, info.name);
            try file.writeAll("\t");
            try writeHex(file, topic_id[0..]);
            try writer.print("\t{d}\t{d}\t{d}\t{d}\t{d}\t{d}\t{d}\t", .{
                info.num_partitions,
                info.replication_factor,
                retention_ms,
                retention_bytes,
                max_message_bytes,
                min_insync_replicas,
                segment_bytes,
            });
            try writeHex(file, cleanup_policy);
            try file.writeAll("\t");
            try writeHex(file, compression_type);
            try file.writeAll("\n");
        }
        try file.sync();
    }

    /// Load topic metadata from disk.
    pub fn loadTopics(self: *MetadataPersistence) ![]TopicEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/topics.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No topics.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read topics.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(TopicEntry).init(self.allocator);

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const first = fields.next() orelse continue;

            if (std.mem.eql(u8, first, "topic_v4")) {
                const name_hex = fields.next() orelse continue;
                const topic_id_hex = fields.next() orelse continue;
                const parts_str = fields.next() orelse continue;
                const rf_str = fields.next() orelse continue;
                const retention_ms_str = fields.next() orelse continue;
                const retention_bytes_str = fields.next() orelse continue;
                const max_message_bytes_str = fields.next() orelse continue;
                const min_insync_replicas_str = fields.next() orelse continue;
                const segment_bytes_str = fields.next() orelse continue;
                const cleanup_policy_hex = fields.next() orelse continue;
                const compression_type_hex = fields.next() orelse continue;

                const name = decodeHexAlloc(self.allocator, name_hex) catch continue;
                const topic_id = decodeUuidHex(topic_id_hex) catch {
                    self.allocator.free(name);
                    continue;
                };
                const num_parts = std.fmt.parseInt(i32, parts_str, 10) catch {
                    self.allocator.free(name);
                    continue;
                };
                const rf = std.fmt.parseInt(i16, rf_str, 10) catch {
                    self.allocator.free(name);
                    continue;
                };
                const retention_ms = std.fmt.parseInt(i64, retention_ms_str, 10) catch default_topic_retention_ms;
                const retention_bytes = std.fmt.parseInt(i64, retention_bytes_str, 10) catch default_topic_retention_bytes;
                const max_message_bytes = std.fmt.parseInt(i32, max_message_bytes_str, 10) catch default_topic_max_message_bytes;
                const min_insync_replicas = std.fmt.parseInt(i32, min_insync_replicas_str, 10) catch default_topic_min_insync_replicas;
                const segment_bytes = std.fmt.parseInt(i64, segment_bytes_str, 10) catch default_topic_segment_bytes;
                const cleanup_policy_owned = decodeHexAlloc(self.allocator, cleanup_policy_hex) catch {
                    self.allocator.free(name);
                    continue;
                };
                defer self.allocator.free(cleanup_policy_owned);
                const compression_type_owned = decodeHexAlloc(self.allocator, compression_type_hex) catch {
                    self.allocator.free(name);
                    continue;
                };
                defer self.allocator.free(compression_type_owned);
                const cleanup_policy = normalizeTopicCleanupPolicy(cleanup_policy_owned) orelse default_topic_cleanup_policy;
                const compression_type = normalizeTopicCompressionType(compression_type_owned) orelse default_topic_compression_type;

                try entries.append(.{
                    .name = name,
                    .num_partitions = num_parts,
                    .replication_factor = rf,
                    .topic_id = topic_id,
                    .retention_ms = retention_ms,
                    .retention_bytes = retention_bytes,
                    .max_message_bytes = max_message_bytes,
                    .min_insync_replicas = min_insync_replicas,
                    .segment_bytes = segment_bytes,
                    .cleanup_policy = cleanup_policy,
                    .compression_type = compression_type,
                });
            } else if (std.mem.eql(u8, first, "topic_v3")) {
                const name_hex = fields.next() orelse continue;
                const topic_id_hex = fields.next() orelse continue;
                const parts_str = fields.next() orelse continue;
                const rf_str = fields.next() orelse continue;
                const retention_ms_str = fields.next() orelse continue;
                const retention_bytes_str = fields.next() orelse continue;
                const max_message_bytes_str = fields.next() orelse continue;
                const min_insync_replicas_str = fields.next() orelse continue;

                const name = decodeHexAlloc(self.allocator, name_hex) catch continue;
                const topic_id = decodeUuidHex(topic_id_hex) catch {
                    self.allocator.free(name);
                    continue;
                };
                const num_parts = std.fmt.parseInt(i32, parts_str, 10) catch {
                    self.allocator.free(name);
                    continue;
                };
                const rf = std.fmt.parseInt(i16, rf_str, 10) catch {
                    self.allocator.free(name);
                    continue;
                };
                const retention_ms = std.fmt.parseInt(i64, retention_ms_str, 10) catch default_topic_retention_ms;
                const retention_bytes = std.fmt.parseInt(i64, retention_bytes_str, 10) catch default_topic_retention_bytes;
                const max_message_bytes = std.fmt.parseInt(i32, max_message_bytes_str, 10) catch default_topic_max_message_bytes;
                const min_insync_replicas = std.fmt.parseInt(i32, min_insync_replicas_str, 10) catch default_topic_min_insync_replicas;

                try entries.append(.{
                    .name = name,
                    .num_partitions = num_parts,
                    .replication_factor = rf,
                    .topic_id = topic_id,
                    .retention_ms = retention_ms,
                    .retention_bytes = retention_bytes,
                    .max_message_bytes = max_message_bytes,
                    .min_insync_replicas = min_insync_replicas,
                });
            } else if (std.mem.eql(u8, first, "topic_v2")) {
                const name_hex = fields.next() orelse continue;
                const parts_str = fields.next() orelse continue;
                const rf_str = fields.next() orelse continue;
                const retention_ms_str = fields.next() orelse continue;
                const retention_bytes_str = fields.next() orelse continue;
                const max_message_bytes_str = fields.next() orelse continue;
                const min_insync_replicas_str = fields.next() orelse continue;

                const name = decodeHexAlloc(self.allocator, name_hex) catch continue;
                const num_parts = std.fmt.parseInt(i32, parts_str, 10) catch {
                    self.allocator.free(name);
                    continue;
                };
                const rf = std.fmt.parseInt(i16, rf_str, 10) catch {
                    self.allocator.free(name);
                    continue;
                };
                const retention_ms = std.fmt.parseInt(i64, retention_ms_str, 10) catch default_topic_retention_ms;
                const retention_bytes = std.fmt.parseInt(i64, retention_bytes_str, 10) catch default_topic_retention_bytes;
                const max_message_bytes = std.fmt.parseInt(i32, max_message_bytes_str, 10) catch default_topic_max_message_bytes;
                const min_insync_replicas = std.fmt.parseInt(i32, min_insync_replicas_str, 10) catch default_topic_min_insync_replicas;

                try entries.append(.{
                    .name = name,
                    .num_partitions = num_parts,
                    .replication_factor = rf,
                    .retention_ms = retention_ms,
                    .retention_bytes = retention_bytes,
                    .max_message_bytes = max_message_bytes,
                    .min_insync_replicas = min_insync_replicas,
                });
            } else {
                const parts_str = fields.next() orelse continue;
                const rf_str = fields.next() orelse continue;

                const num_parts = std.fmt.parseInt(i32, parts_str, 10) catch continue;
                const rf = std.fmt.parseInt(i16, rf_str, 10) catch continue;

                try entries.append(.{
                    .name = try self.allocator.dupe(u8, first),
                    .num_partitions = num_parts,
                    .replication_factor = rf,
                });
            }
        }

        log.info("Loaded {d} topics from topics.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    /// Save committed offsets to disk.
    pub fn saveOffsets(self: *MetadataPersistence, offsets: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/offsets.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        const writer = file.writer();
        var it = offsets.iterator();
        while (it.next()) |entry| {
            try writer.print("{s}\t{d}\t{d}\t", .{ entry.key_ptr.*, entry.value_ptr.offset, entry.value_ptr.leader_epoch });
            if (entry.value_ptr.metadata) |metadata| {
                try file.writeAll("1\t");
                try writeHex(file, metadata);
            } else {
                try file.writeAll("0\t");
            }
            try file.writeAll("\n");
        }
        try file.sync();
    }

    /// Load committed offsets from disk.
    pub fn loadOffsets(self: *MetadataPersistence) ![]OffsetEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/offsets.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No offsets.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read offsets.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(OffsetEntry).init(self.allocator);

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            var fields = std.mem.splitSequence(u8, line, "\t");
            const key = fields.next() orelse continue;
            const val_str = fields.next() orelse continue;
            const offset = std.fmt.parseInt(i64, val_str, 10) catch continue;
            const leader_epoch_str = fields.next() orelse "";
            const leader_epoch = if (leader_epoch_str.len > 0)
                std.fmt.parseInt(i32, leader_epoch_str, 10) catch -1
            else
                -1;
            const has_metadata_str = fields.next() orelse "0";
            const metadata_hex = fields.next() orelse "";
            const metadata = decodeOptionalHexAlloc(self.allocator, has_metadata_str, metadata_hex) catch continue;
            try entries.append(.{
                .key = try self.allocator.dupe(u8, key),
                .offset = offset,
                .leader_epoch = leader_epoch,
                .metadata = metadata,
            });
        }

        log.info("Loaded {d} offsets from offsets.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    /// Save consumer-group lifecycle metadata to disk.
    /// Format: nested TSV with hex-encoded IDs and binary assignments.
    pub fn saveConsumerGroups(self: *MetadataPersistence, groups: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/consumer_groups.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        var it = groups.iterator();
        while (it.next()) |entry| {
            const group = entry.value_ptr;
            try file.writeAll("group_v1\t");
            try writeHex(file, group.group_id);
            try writer.print("\t{d}\t{d}\t{d}\t", .{
                @intFromEnum(group.state),
                group.generation_id,
                group.next_member_id,
            });
            if (group.leader_id) |leader_id| {
                try file.writeAll("1\t");
                try writeHex(file, leader_id);
            } else {
                try file.writeAll("0\t");
            }
            try writer.print("\t{d}\t{d}\t{d}", .{
                group.rebalance_timeout_ms,
                group.session_timeout_ms,
                group.members.count(),
            });
            if (group.protocol_type) |protocol_type| {
                try file.writeAll("\t1\t");
                try writeHex(file, protocol_type);
            } else {
                try file.writeAll("\t0\t");
            }
            if (group.protocol_name) |protocol_name| {
                try file.writeAll("\t1\t");
                try writeHex(file, protocol_name);
            } else {
                try file.writeAll("\t0\t");
            }
            try file.writeAll("\n");

            var member_it = group.members.iterator();
            while (member_it.next()) |member_entry| {
                const member = member_entry.value_ptr;
                try file.writeAll("member_v1\t");
                try writeHex(file, member.member_id);

                if (member.group_instance_id) |group_instance_id| {
                    try file.writeAll("\t1\t");
                    try writeHex(file, group_instance_id);
                } else {
                    try file.writeAll("\t0\t");
                }

                try writer.print("\t{d}\t", .{member.last_heartbeat_ms});

                if (member.assignment) |assignment| {
                    try file.writeAll("1\t");
                    try writeHex(file, assignment);
                } else {
                    try file.writeAll("0\t");
                }

                if (member.protocol_name) |protocol_name| {
                    try file.writeAll("\t1\t");
                    try writeHex(file, protocol_name);
                } else {
                    try file.writeAll("\t0\t");
                }

                try writer.print("\t{d}", .{member.subscribed_topics.items.len});
                for (member.subscribed_topics.items) |subscription| {
                    try file.writeAll("\t");
                    try writeHex(file, subscription);
                }
                if (member.protocol_metadata) |protocol_metadata| {
                    try file.writeAll("\t1\t");
                    try writeHex(file, protocol_metadata);
                } else {
                    try file.writeAll("\t0\t");
                }
                if (member.rack_id) |rack_id| {
                    try file.writeAll("\t1\t");
                    try writeHex(file, rack_id);
                } else {
                    try file.writeAll("\t0\t");
                }
                try file.writeAll("\n");
            }
        }
        try file.sync();
    }

    /// Load consumer-group lifecycle metadata from disk.
    pub fn loadConsumerGroups(self: *MetadataPersistence) ![]ConsumerGroupEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/consumer_groups.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No consumer_groups.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 4 * 1024 * 1024) catch |err| {
            log.warn("Failed to read consumer_groups.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(ConsumerGroupEntry).init(self.allocator);
        errdefer {
            self.freeConsumerGroupMemberEntriesFromGroups(entries.items);
            entries.deinit();
        }

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;
            if (!std.mem.eql(u8, tag, "group_v1")) continue;

            const group_id_hex = fields.next() orelse continue;
            const state_str = fields.next() orelse continue;
            const generation_str = fields.next() orelse continue;
            const next_member_id_str = fields.next() orelse continue;
            const has_leader_str = fields.next() orelse continue;
            const leader_hex = fields.next() orelse "";
            const rebalance_timeout_str = fields.next() orelse continue;
            const session_timeout_str = fields.next() orelse continue;
            const member_count_str = fields.next() orelse continue;

            const group_id = decodeHexAlloc(self.allocator, group_id_hex) catch continue;
            const leader_id = decodeOptionalHexAlloc(self.allocator, has_leader_str, leader_hex) catch {
                self.allocator.free(group_id);
                continue;
            };

            const state = std.fmt.parseInt(u8, state_str, 10) catch {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                continue;
            };
            const generation_id = std.fmt.parseInt(i32, generation_str, 10) catch {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                continue;
            };
            const next_member_id = std.fmt.parseInt(u64, next_member_id_str, 10) catch {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                continue;
            };
            const rebalance_timeout_ms = std.fmt.parseInt(i64, rebalance_timeout_str, 10) catch {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                continue;
            };
            const session_timeout_ms = std.fmt.parseInt(i64, session_timeout_str, 10) catch {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                continue;
            };
            const member_count = std.fmt.parseInt(usize, member_count_str, 10) catch {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                continue;
            };
            var group_protocol_type: ?[]u8 = null;
            if (fields.next()) |has_protocol_type_str| {
                const protocol_type_hex = fields.next() orelse "";
                group_protocol_type = decodeOptionalHexAlloc(self.allocator, has_protocol_type_str, protocol_type_hex) catch {
                    self.allocator.free(group_id);
                    if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                    continue;
                };
            }
            var group_protocol_name: ?[]u8 = null;
            if (fields.next()) |has_protocol_name_str| {
                const protocol_name_hex = fields.next() orelse "";
                group_protocol_name = decodeOptionalHexAlloc(self.allocator, has_protocol_name_str, protocol_name_hex) catch {
                    self.allocator.free(group_id);
                    if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                    if (group_protocol_type) |owned_protocol_type| self.allocator.free(owned_protocol_type);
                    continue;
                };
            }

            var members = std.array_list.Managed(ConsumerGroupMemberEntry).init(self.allocator);
            defer members.deinit();
            var valid_group = true;
            for (0..member_count) |_| {
                const member_line = lines.next() orelse {
                    valid_group = false;
                    break;
                };
                var member_fields = std.mem.splitSequence(u8, member_line, "\t");
                const member_tag = member_fields.next() orelse {
                    valid_group = false;
                    break;
                };
                if (!std.mem.eql(u8, member_tag, "member_v1")) {
                    valid_group = false;
                    break;
                }

                const member_id_hex = member_fields.next() orelse {
                    valid_group = false;
                    break;
                };
                const has_instance_str = member_fields.next() orelse {
                    valid_group = false;
                    break;
                };
                const instance_hex = member_fields.next() orelse "";
                const heartbeat_str = member_fields.next() orelse {
                    valid_group = false;
                    break;
                };
                const has_assignment_str = member_fields.next() orelse {
                    valid_group = false;
                    break;
                };
                const assignment_hex = member_fields.next() orelse "";
                const has_protocol_str = member_fields.next() orelse {
                    valid_group = false;
                    break;
                };
                const protocol_hex = member_fields.next() orelse "";
                const subscription_count_str = member_fields.next() orelse {
                    valid_group = false;
                    break;
                };

                const member_id = decodeHexAlloc(self.allocator, member_id_hex) catch {
                    valid_group = false;
                    break;
                };
                const group_instance_id = decodeOptionalHexAlloc(self.allocator, has_instance_str, instance_hex) catch {
                    self.allocator.free(member_id);
                    valid_group = false;
                    break;
                };
                const last_heartbeat_ms = std.fmt.parseInt(i64, heartbeat_str, 10) catch {
                    self.allocator.free(member_id);
                    if (group_instance_id) |owned_instance| self.allocator.free(owned_instance);
                    valid_group = false;
                    break;
                };
                const assignment = decodeOptionalHexAlloc(self.allocator, has_assignment_str, assignment_hex) catch {
                    self.allocator.free(member_id);
                    if (group_instance_id) |owned_instance| self.allocator.free(owned_instance);
                    valid_group = false;
                    break;
                };
                const protocol_name = decodeOptionalHexAlloc(self.allocator, has_protocol_str, protocol_hex) catch {
                    self.allocator.free(member_id);
                    if (group_instance_id) |owned_instance| self.allocator.free(owned_instance);
                    if (assignment) |owned_assignment| self.allocator.free(owned_assignment);
                    valid_group = false;
                    break;
                };
                const subscription_count = std.fmt.parseInt(usize, subscription_count_str, 10) catch {
                    self.allocator.free(member_id);
                    if (group_instance_id) |owned_instance| self.allocator.free(owned_instance);
                    if (assignment) |owned_assignment| self.allocator.free(owned_assignment);
                    if (protocol_name) |owned_protocol| self.allocator.free(owned_protocol);
                    valid_group = false;
                    break;
                };

                var subscriptions = std.array_list.Managed([]u8).init(self.allocator);
                defer subscriptions.deinit();
                for (0..subscription_count) |_| {
                    const subscription_hex = member_fields.next() orelse {
                        valid_group = false;
                        break;
                    };
                    const subscription = decodeHexAlloc(self.allocator, subscription_hex) catch {
                        valid_group = false;
                        break;
                    };
                    subscriptions.append(subscription) catch |err| {
                        self.allocator.free(subscription);
                        return err;
                    };
                }
                var protocol_metadata: ?[]u8 = null;
                if (member_fields.next()) |has_metadata_str| {
                    const metadata_hex = member_fields.next() orelse "";
                    protocol_metadata = decodeOptionalHexAlloc(self.allocator, has_metadata_str, metadata_hex) catch blk: {
                        valid_group = false;
                        break :blk null;
                    };
                }
                var rack_id: ?[]u8 = null;
                if (member_fields.next()) |has_rack_str| {
                    const rack_hex = member_fields.next() orelse "";
                    rack_id = decodeOptionalHexAlloc(self.allocator, has_rack_str, rack_hex) catch blk: {
                        valid_group = false;
                        break :blk null;
                    };
                }
                if (!valid_group) {
                    self.allocator.free(member_id);
                    if (group_instance_id) |owned_instance| self.allocator.free(owned_instance);
                    if (assignment) |owned_assignment| self.allocator.free(owned_assignment);
                    if (protocol_name) |owned_protocol| self.allocator.free(owned_protocol);
                    if (protocol_metadata) |owned_metadata| self.allocator.free(owned_metadata);
                    if (rack_id) |owned_rack| self.allocator.free(owned_rack);
                    for (subscriptions.items) |subscription| self.allocator.free(subscription);
                    break;
                }

                const owned_subscriptions = try subscriptions.toOwnedSlice();
                members.append(.{
                    .member_id = member_id,
                    .group_instance_id = group_instance_id,
                    .last_heartbeat_ms = last_heartbeat_ms,
                    .assignment = assignment,
                    .protocol_name = protocol_name,
                    .protocol_metadata = protocol_metadata,
                    .rack_id = rack_id,
                    .subscriptions = owned_subscriptions,
                }) catch |err| {
                    self.allocator.free(member_id);
                    if (group_instance_id) |owned_instance| self.allocator.free(owned_instance);
                    if (assignment) |owned_assignment| self.allocator.free(owned_assignment);
                    if (protocol_name) |owned_protocol| self.allocator.free(owned_protocol);
                    if (protocol_metadata) |owned_metadata| self.allocator.free(owned_metadata);
                    if (rack_id) |owned_rack| self.allocator.free(owned_rack);
                    for (owned_subscriptions) |subscription| self.allocator.free(subscription);
                    if (owned_subscriptions.len > 0) self.allocator.free(owned_subscriptions);
                    return err;
                };
            }

            if (!valid_group) {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                if (group_protocol_type) |owned_protocol_type| self.allocator.free(owned_protocol_type);
                if (group_protocol_name) |owned_protocol_name| self.allocator.free(owned_protocol_name);
                self.freeConsumerGroupMemberEntries(members.items);
                continue;
            }

            const owned_members = try members.toOwnedSlice();
            entries.append(.{
                .group_id = group_id,
                .state = state,
                .generation_id = generation_id,
                .next_member_id = next_member_id,
                .leader_id = leader_id,
                .protocol_type = group_protocol_type,
                .protocol_name = group_protocol_name,
                .rebalance_timeout_ms = rebalance_timeout_ms,
                .session_timeout_ms = session_timeout_ms,
                .members = owned_members,
            }) catch |err| {
                self.allocator.free(group_id);
                if (leader_id) |owned_leader| self.allocator.free(owned_leader);
                if (group_protocol_type) |owned_protocol_type| self.allocator.free(owned_protocol_type);
                if (group_protocol_name) |owned_protocol_name| self.allocator.free(owned_protocol_name);
                self.freeConsumerGroupMemberEntries(owned_members);
                if (owned_members.len > 0) self.allocator.free(owned_members);
                return err;
            };
        }

        log.info("Loaded {d} consumer group(s) from consumer_groups.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    pub const TopicEntry = struct {
        name: []u8,
        num_partitions: i32,
        replication_factor: i16,
        topic_id: [16]u8 = [_]u8{0} ** 16,
        retention_ms: i64 = default_topic_retention_ms,
        retention_bytes: i64 = default_topic_retention_bytes,
        max_message_bytes: i32 = default_topic_max_message_bytes,
        min_insync_replicas: i32 = default_topic_min_insync_replicas,
        segment_bytes: i64 = default_topic_segment_bytes,
        cleanup_policy: []const u8 = default_topic_cleanup_policy,
        compression_type: []const u8 = default_topic_compression_type,
    };

    pub const OffsetEntry = struct {
        key: []u8,
        offset: i64,
        leader_epoch: i32 = -1,
        metadata: ?[]u8 = null,
    };

    pub fn freeOffsetEntries(self: *MetadataPersistence, entries: []OffsetEntry) void {
        for (entries) |entry| {
            self.allocator.free(entry.key);
            if (entry.metadata) |metadata| self.allocator.free(metadata);
        }
        if (entries.len > 0) self.allocator.free(entries);
    }

    pub const ConsumerGroupMemberEntry = struct {
        member_id: []u8,
        group_instance_id: ?[]u8,
        last_heartbeat_ms: i64,
        assignment: ?[]u8,
        protocol_name: ?[]u8,
        protocol_metadata: ?[]u8,
        rack_id: ?[]u8,
        subscriptions: [][]u8,
    };

    pub const ConsumerGroupEntry = struct {
        group_id: []u8,
        state: u8,
        generation_id: i32,
        next_member_id: u64,
        leader_id: ?[]u8,
        protocol_type: ?[]u8,
        protocol_name: ?[]u8,
        rebalance_timeout_ms: i64,
        session_timeout_ms: i64,
        members: []ConsumerGroupMemberEntry,
    };

    fn freeConsumerGroupMemberEntries(self: *MetadataPersistence, members: []ConsumerGroupMemberEntry) void {
        for (members) |member| {
            self.allocator.free(member.member_id);
            if (member.group_instance_id) |group_instance_id| self.allocator.free(group_instance_id);
            if (member.assignment) |assignment| self.allocator.free(assignment);
            if (member.protocol_name) |protocol_name| self.allocator.free(protocol_name);
            if (member.protocol_metadata) |protocol_metadata| self.allocator.free(protocol_metadata);
            if (member.rack_id) |rack_id| self.allocator.free(rack_id);
            for (member.subscriptions) |subscription| self.allocator.free(subscription);
            if (member.subscriptions.len > 0) self.allocator.free(member.subscriptions);
        }
    }

    fn freeConsumerGroupMemberEntriesFromGroups(self: *MetadataPersistence, entries: []ConsumerGroupEntry) void {
        for (entries) |entry| {
            self.allocator.free(entry.group_id);
            if (entry.leader_id) |leader_id| self.allocator.free(leader_id);
            if (entry.protocol_type) |protocol_type| self.allocator.free(protocol_type);
            if (entry.protocol_name) |protocol_name| self.allocator.free(protocol_name);
            self.freeConsumerGroupMemberEntries(entry.members);
            if (entry.members.len > 0) self.allocator.free(entry.members);
        }
    }

    pub fn freeConsumerGroupEntries(self: *MetadataPersistence, entries: []ConsumerGroupEntry) void {
        self.freeConsumerGroupMemberEntriesFromGroups(entries);
        if (entries.len > 0) self.allocator.free(entries);
    }

    pub const TransactionPartitionEntry = struct {
        topic: []const u8,
        partition: i32,
    };

    pub const TransactionEntry = struct {
        producer_id: i64,
        producer_epoch: i16,
        status: u8,
        timeout_ms: i32,
        transactional_id: ?[]u8,
        partitions: []const TransactionPartitionEntry = &.{},
    };

    pub const TransactionSnapshot = struct {
        next_producer_id: i64,
        entries: []TransactionEntry,
    };

    pub fn freeTransactionSnapshot(self: *MetadataPersistence, snapshot: TransactionSnapshot) void {
        for (snapshot.entries) |entry| {
            if (entry.transactional_id) |tid| self.allocator.free(tid);
            for (entry.partitions) |partition| self.allocator.free(partition.topic);
            if (entry.partitions.len > 0) self.allocator.free(entry.partitions);
        }
        if (snapshot.entries.len > 0) self.allocator.free(snapshot.entries);
    }

    pub const ProducerSequenceEntry = struct {
        producer_id: i64,
        partition_key: u64,
        last_sequence: i32,
        producer_epoch: i16,
    };

    pub const PartitionStateEntry = struct {
        topic: []u8,
        partition_id: i32,
        next_offset: u64,
        log_start_offset: u64,
        high_watermark: u64,
        last_stable_offset: u64,
        first_unstable_txn_offset: ?u64,
    };

    pub const PartitionReassignmentEntry = struct {
        topic: []u8,
        partition_index: i32,
        replicas: []i32,
        adding_replicas: []i32,
        removing_replicas: []i32,
    };

    pub const ReplicaDirectoryAssignmentEntry = struct {
        topic_id: [16]u8,
        partition_index: i32,
        directory_id: [16]u8,
    };

    pub const ShareStateBatchEntry = struct {
        first_offset: i64,
        last_offset: i64,
        delivery_state: i8,
        delivery_count: i16,
    };

    pub const ShareGroupStateEntry = struct {
        key: []u8,
        state_epoch: i32,
        start_offset: i64,
        batches: []ShareStateBatchEntry,
    };

    pub const ShareGroupSessionEntry = struct {
        key: []u8,
        epoch: i32,
    };

    pub const FinalizedFeatureEntry = struct {
        name: []u8,
        max_version_level: i16,
    };

    pub const FinalizedFeatureSnapshot = struct {
        epoch: i64,
        features: []FinalizedFeatureEntry,
    };

    /// Save transaction state to disk.
    /// NOTE: AutoMQ/Kafka persists to __transaction_state topic on coordinator startup.
    /// ZMQ uses file-based persistence as a simplification.
    pub fn saveTransactions(self: *MetadataPersistence, coordinator: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/transactions.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        const writer = file.writer();
        try writer.print("next_producer_id\t{d}\n", .{coordinator.next_producer_id});

        var it = coordinator.transactions.iterator();
        while (it.next()) |entry| {
            const txn = entry.value_ptr;
            try writer.print("txn_v2\t{d}\t{d}\t{d}\t{d}\t", .{
                txn.producer_id,
                txn.producer_epoch,
                @intFromEnum(txn.status),
                txn.timeout_ms,
            });
            if (txn.transactional_id) |tid| try writeHex(file, tid);
            try writer.print("\t{d}", .{txn.partitions.items.len});
            for (txn.partitions.items) |partition| {
                try file.writeAll("\t");
                try writeHex(file, partition.topic);
                try writer.print("\t{d}", .{partition.partition});
            }
            try file.writeAll("\n");
        }
        try file.sync();
    }

    /// Load transaction state from disk.
    /// Returns default snapshot (next_producer_id=1000, empty entries) if file is missing.
    pub fn loadTransactions(self: *MetadataPersistence) !TransactionSnapshot {
        const dir = self.data_dir orelse return .{ .next_producer_id = 1000, .entries = &.{} };

        const path = try std.fmt.allocPrint(self.allocator, "{s}/transactions.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No transactions.meta found: {}", .{err});
            return .{ .next_producer_id = 1000, .entries = &.{} };
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read transactions.meta: {}", .{err});
            return .{ .next_producer_id = 1000, .entries = &.{} };
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(TransactionEntry).init(self.allocator);
        errdefer {
            for (entries.items) |entry| {
                if (entry.transactional_id) |tid| self.allocator.free(tid);
                for (entry.partitions) |partition| self.allocator.free(partition.topic);
                if (entry.partitions.len > 0) self.allocator.free(entry.partitions);
            }
            entries.deinit();
        }
        var next_pid: i64 = 1000;

        var lines = std.mem.splitSequence(u8, content, "\n");
        // First line: next_producer_id\t{value}
        if (lines.next()) |first_line| {
            if (first_line.len > 0) {
                var fields = std.mem.splitSequence(u8, first_line, "\t");
                _ = fields.next(); // skip "next_producer_id" label
                if (fields.next()) |val_str| {
                    next_pid = std.fmt.parseInt(i64, val_str, 10) catch 1000;
                }
            }
        }

        // Remaining lines: txn_v2 rows, or legacy producer_id rows from older snapshots.
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const first = fields.next() orelse continue;

            if (std.mem.eql(u8, first, "txn_v2")) {
                const pid_str = fields.next() orelse continue;
                const epoch_str = fields.next() orelse continue;
                const status_str = fields.next() orelse continue;
                const timeout_str = fields.next() orelse continue;
                const tid_hex = fields.next() orelse continue;
                const partition_count_str = fields.next() orelse continue;

                const pid = std.fmt.parseInt(i64, pid_str, 10) catch continue;
                const epoch = std.fmt.parseInt(i16, epoch_str, 10) catch continue;
                const status = std.fmt.parseInt(u8, status_str, 10) catch continue;
                const timeout = std.fmt.parseInt(i32, timeout_str, 10) catch continue;
                const partition_count = std.fmt.parseInt(usize, partition_count_str, 10) catch continue;

                const tid: ?[]u8 = if (tid_hex.len > 0)
                    decodeHexAlloc(self.allocator, tid_hex) catch continue
                else
                    null;
                errdefer if (tid) |owned_tid| self.allocator.free(owned_tid);

                var partitions = std.array_list.Managed(TransactionPartitionEntry).init(self.allocator);
                defer partitions.deinit();
                errdefer for (partitions.items) |partition| self.allocator.free(partition.topic);
                var valid_partitions = true;
                for (0..partition_count) |_| {
                    const topic_hex = fields.next() orelse {
                        valid_partitions = false;
                        break;
                    };
                    const partition_str = fields.next() orelse {
                        valid_partitions = false;
                        break;
                    };
                    const topic = decodeHexAlloc(self.allocator, topic_hex) catch {
                        valid_partitions = false;
                        break;
                    };
                    errdefer self.allocator.free(topic);
                    const partition = std.fmt.parseInt(i32, partition_str, 10) catch {
                        self.allocator.free(topic);
                        valid_partitions = false;
                        break;
                    };
                    try partitions.append(.{ .topic = topic, .partition = partition });
                }
                if (!valid_partitions) {
                    for (partitions.items) |partition| self.allocator.free(partition.topic);
                    if (tid) |owned_tid| self.allocator.free(owned_tid);
                    continue;
                }

                const owned_partitions = try partitions.toOwnedSlice();
                errdefer {
                    for (owned_partitions) |partition| self.allocator.free(partition.topic);
                    if (owned_partitions.len > 0) self.allocator.free(owned_partitions);
                }
                try entries.append(.{
                    .producer_id = pid,
                    .producer_epoch = epoch,
                    .status = status,
                    .timeout_ms = timeout,
                    .transactional_id = tid,
                    .partitions = owned_partitions,
                });
                continue;
            }

            const pid_str = first;
            const epoch_str = fields.next() orelse continue;
            const status_str = fields.next() orelse continue;
            const timeout_str = fields.next() orelse continue;
            const tid_str = fields.next() orelse continue;

            const pid = std.fmt.parseInt(i64, pid_str, 10) catch continue;
            const epoch = std.fmt.parseInt(i16, epoch_str, 10) catch continue;
            const status = std.fmt.parseInt(u8, status_str, 10) catch continue;
            const timeout = std.fmt.parseInt(i32, timeout_str, 10) catch continue;

            const tid: ?[]u8 = if (tid_str.len > 0)
                try self.allocator.dupe(u8, tid_str)
            else
                null;
            errdefer if (tid) |owned_tid| self.allocator.free(owned_tid);

            try entries.append(.{
                .producer_id = pid,
                .producer_epoch = epoch,
                .status = status,
                .timeout_ms = timeout,
                .transactional_id = tid,
            });
        }

        log.info("Loaded {d} transactions from transactions.meta (next_pid={d})", .{ entries.items.len, next_pid });
        return .{
            .next_producer_id = next_pid,
            .entries = try entries.toOwnedSlice(),
        };
    }

    /// Save producer sequence state to disk.
    pub fn saveProducerSequences(self: *MetadataPersistence, sequences: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/producer_state.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        const writer = file.writer();
        var it = sequences.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const value = entry.value_ptr.*;
            const producer_id = if (comptime hasComptimeField(@TypeOf(key), "producer_id"))
                key.producer_id
            else
                value.producer_id;
            const partition_key = if (comptime hasComptimeField(@TypeOf(key), "partition_key"))
                key.partition_key
            else
                key;
            try writer.print("{d}\t{d}\t{d}\t{d}\n", .{
                producer_id,
                partition_key,
                value.last_sequence,
                value.producer_epoch,
            });
        }
        try file.sync();
    }

    /// Load producer sequence state from disk.
    /// Returns empty slice if file is missing.
    pub fn loadProducerSequences(self: *MetadataPersistence) ![]ProducerSequenceEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/producer_state.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No producer_state.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read producer_state.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(ProducerSequenceEntry).init(self.allocator);

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const pid_str = fields.next() orelse continue;
            const pkey_str = fields.next() orelse continue;
            const seq_str = fields.next() orelse continue;
            const epoch_str = fields.next() orelse continue;

            const pid = std.fmt.parseInt(i64, pid_str, 10) catch continue;
            const pkey = std.fmt.parseInt(u64, pkey_str, 10) catch continue;
            const seq = std.fmt.parseInt(i32, seq_str, 10) catch continue;
            const epoch = std.fmt.parseInt(i16, epoch_str, 10) catch continue;

            try entries.append(.{
                .producer_id = pid,
                .partition_key = pkey,
                .last_sequence = seq,
                .producer_epoch = epoch,
            });
        }

        log.info("Loaded {d} producer sequences from producer_state.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    /// Save per-partition offset and visibility state to disk.
    /// Format: partition_state.meta TSV with hex-encoded topic names.
    pub fn savePartitionStates(self: *MetadataPersistence, partitions: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/partition_state.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        var it = partitions.iterator();
        while (it.next()) |entry| {
            const state = entry.value_ptr;
            try file.writeAll("partition\t");
            try writeHex(file, state.topic);
            try writer.print("\t{d}\t{d}\t{d}\t{d}\t{d}\t", .{
                state.partition_id,
                state.next_offset,
                state.log_start_offset,
                state.high_watermark,
                state.last_stable_offset,
            });
            if (state.first_unstable_txn_offset) |offset| {
                try writer.print("{d}\n", .{offset});
            } else {
                try file.writeAll("null\n");
            }
        }
        try file.sync();
    }

    /// Load per-partition offset and visibility state from disk.
    pub fn loadPartitionStates(self: *MetadataPersistence) ![]PartitionStateEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/partition_state.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No partition_state.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 4 * 1024 * 1024) catch |err| {
            log.warn("Failed to read partition_state.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(PartitionStateEntry).init(self.allocator);
        errdefer {
            for (entries.items) |entry| self.allocator.free(entry.topic);
            entries.deinit();
        }

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;
            if (!std.mem.eql(u8, tag, "partition")) continue;

            const topic_hex = fields.next() orelse continue;
            const partition_str = fields.next() orelse continue;
            const next_offset_str = fields.next() orelse continue;
            const log_start_str = fields.next() orelse continue;
            const hw_str = fields.next() orelse continue;
            const lso_str = fields.next() orelse continue;
            const unstable_str = fields.next() orelse "null";

            const topic = decodeHexAlloc(self.allocator, topic_hex) catch continue;
            const partition_id = std.fmt.parseInt(i32, partition_str, 10) catch {
                self.allocator.free(topic);
                continue;
            };
            const next_offset = std.fmt.parseInt(u64, next_offset_str, 10) catch {
                self.allocator.free(topic);
                continue;
            };
            const log_start_offset = std.fmt.parseInt(u64, log_start_str, 10) catch {
                self.allocator.free(topic);
                continue;
            };
            const high_watermark = std.fmt.parseInt(u64, hw_str, 10) catch {
                self.allocator.free(topic);
                continue;
            };
            const last_stable_offset = std.fmt.parseInt(u64, lso_str, 10) catch {
                self.allocator.free(topic);
                continue;
            };
            const first_unstable_txn_offset: ?u64 = if (std.mem.eql(u8, unstable_str, "null"))
                null
            else
                std.fmt.parseInt(u64, unstable_str, 10) catch null;

            entries.append(.{
                .topic = topic,
                .partition_id = partition_id,
                .next_offset = next_offset,
                .log_start_offset = log_start_offset,
                .high_watermark = high_watermark,
                .last_stable_offset = last_stable_offset,
                .first_unstable_txn_offset = first_unstable_txn_offset,
            }) catch |err| {
                self.allocator.free(topic);
                return err;
            };
        }

        log.info("Loaded {d} partition states from partition_state.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    /// Save ongoing partition reassignment metadata to disk.
    /// Format: partition_reassignments.meta TSV with hex-encoded topic names and CSV replica sets.
    pub fn savePartitionReassignments(self: *MetadataPersistence, reassignments: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/partition_reassignments.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        var it = reassignments.iterator();
        while (it.next()) |entry| {
            const state = entry.value_ptr;
            try file.writeAll("reassignment\t");
            try writeHex(file, state.topic);
            try writer.print("\t{d}\t", .{state.partition_index});
            try writeI32Csv(file, state.replicas);
            try file.writeAll("\t");
            try writeI32Csv(file, state.adding_replicas);
            try file.writeAll("\t");
            try writeI32Csv(file, state.removing_replicas);
            try file.writeAll("\n");
        }
        try file.sync();
    }

    /// Load ongoing partition reassignment metadata from disk.
    pub fn loadPartitionReassignments(self: *MetadataPersistence) ![]PartitionReassignmentEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/partition_reassignments.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No partition_reassignments.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 4 * 1024 * 1024) catch |err| {
            log.warn("Failed to read partition_reassignments.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(PartitionReassignmentEntry).init(self.allocator);
        errdefer {
            for (entries.items) |entry| {
                self.allocator.free(entry.topic);
                if (entry.replicas.len > 0) self.allocator.free(entry.replicas);
                if (entry.adding_replicas.len > 0) self.allocator.free(entry.adding_replicas);
                if (entry.removing_replicas.len > 0) self.allocator.free(entry.removing_replicas);
            }
            entries.deinit();
        }

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;
            if (!std.mem.eql(u8, tag, "reassignment")) continue;

            const topic_hex = fields.next() orelse continue;
            const partition_str = fields.next() orelse continue;
            const replicas_str = fields.next() orelse continue;
            const adding_str = fields.next() orelse continue;
            const removing_str = fields.next() orelse continue;

            const topic = decodeHexAlloc(self.allocator, topic_hex) catch continue;
            const partition_index = std.fmt.parseInt(i32, partition_str, 10) catch {
                self.allocator.free(topic);
                continue;
            };
            const replicas = parseI32CsvAlloc(self.allocator, replicas_str) catch {
                self.allocator.free(topic);
                continue;
            };
            const adding_replicas = parseI32CsvAlloc(self.allocator, adding_str) catch {
                self.allocator.free(topic);
                if (replicas.len > 0) self.allocator.free(replicas);
                continue;
            };
            const removing_replicas = parseI32CsvAlloc(self.allocator, removing_str) catch {
                self.allocator.free(topic);
                if (replicas.len > 0) self.allocator.free(replicas);
                if (adding_replicas.len > 0) self.allocator.free(adding_replicas);
                continue;
            };

            entries.append(.{
                .topic = topic,
                .partition_index = partition_index,
                .replicas = replicas,
                .adding_replicas = adding_replicas,
                .removing_replicas = removing_replicas,
            }) catch |err| {
                self.allocator.free(topic);
                if (replicas.len > 0) self.allocator.free(replicas);
                if (adding_replicas.len > 0) self.allocator.free(adding_replicas);
                if (removing_replicas.len > 0) self.allocator.free(removing_replicas);
                return err;
            };
        }

        log.info("Loaded {d} partition reassignments from partition_reassignments.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    pub fn freePartitionReassignmentEntries(self: *MetadataPersistence, entries: []PartitionReassignmentEntry) void {
        for (entries) |entry| {
            self.allocator.free(entry.topic);
            if (entry.replicas.len > 0) self.allocator.free(entry.replicas);
            if (entry.adding_replicas.len > 0) self.allocator.free(entry.adding_replicas);
            if (entry.removing_replicas.len > 0) self.allocator.free(entry.removing_replicas);
        }
        if (entries.len > 0) self.allocator.free(entries);
    }

    /// Save local replica-directory assignments to disk.
    /// Format: replica_directory_assignments.meta TSV with hex topic/directory UUIDs.
    pub fn saveReplicaDirectoryAssignments(self: *MetadataPersistence, assignments: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/replica_directory_assignments.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        var it = assignments.iterator();
        while (it.next()) |entry| {
            const assignment = entry.value_ptr;
            try file.writeAll("replica_dir\t");
            try writeHex(file, assignment.topic_id[0..]);
            try writer.print("\t{d}\t", .{assignment.partition_index});
            try writeHex(file, assignment.directory_id[0..]);
            try file.writeAll("\n");
        }
        try file.sync();
    }

    pub fn loadReplicaDirectoryAssignments(self: *MetadataPersistence) ![]ReplicaDirectoryAssignmentEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/replica_directory_assignments.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No replica_directory_assignments.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read replica_directory_assignments.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(ReplicaDirectoryAssignmentEntry).init(self.allocator);
        errdefer entries.deinit();

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;
            if (!std.mem.eql(u8, tag, "replica_dir")) continue;

            const topic_id_hex = fields.next() orelse continue;
            const partition_str = fields.next() orelse continue;
            const directory_id_hex = fields.next() orelse continue;

            const topic_id = decodeUuidHex(topic_id_hex) catch continue;
            const partition_index = std.fmt.parseInt(i32, partition_str, 10) catch continue;
            const directory_id = decodeUuidHex(directory_id_hex) catch continue;

            try entries.append(.{
                .topic_id = topic_id,
                .partition_index = partition_index,
                .directory_id = directory_id,
            });
        }

        log.info("Loaded {d} replica directory assignments from replica_directory_assignments.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    pub fn freeReplicaDirectoryAssignmentEntries(self: *MetadataPersistence, entries: []ReplicaDirectoryAssignmentEntry) void {
        if (entries.len > 0) self.allocator.free(entries);
    }

    /// Save local share-group partition state to disk.
    /// Format: share_group_states.meta TSV with a hex-encoded internal state key.
    pub fn saveShareGroupStates(self: *MetadataPersistence, states: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/share_group_states.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        var it = states.iterator();
        while (it.next()) |entry| {
            const state = entry.value_ptr;
            try file.writeAll("share_state\t");
            try writeHex(file, entry.key_ptr.*);
            try writer.print("\t{d}\t{d}\t{d}", .{
                state.state_epoch,
                state.start_offset,
                state.batches.len,
            });
            for (state.batches) |batch| {
                try writer.print("\t{d}\t{d}\t{d}\t{d}", .{
                    batch.first_offset,
                    batch.last_offset,
                    batch.delivery_state,
                    batch.delivery_count,
                });
            }
            try file.writeAll("\n");
        }
        try file.sync();
    }

    pub fn loadShareGroupStates(self: *MetadataPersistence) ![]ShareGroupStateEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/share_group_states.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No share_group_states.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 4 * 1024 * 1024) catch |err| {
            log.warn("Failed to read share_group_states.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(ShareGroupStateEntry).init(self.allocator);
        errdefer {
            for (entries.items) |entry| {
                self.allocator.free(entry.key);
                if (entry.batches.len > 0) self.allocator.free(entry.batches);
            }
            entries.deinit();
        }

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;
            if (!std.mem.eql(u8, tag, "share_state")) continue;

            const key_hex = fields.next() orelse continue;
            const state_epoch_str = fields.next() orelse continue;
            const start_offset_str = fields.next() orelse continue;
            const batch_count_str = fields.next() orelse continue;

            const key = decodeHexAlloc(self.allocator, key_hex) catch continue;
            const state_epoch = std.fmt.parseInt(i32, state_epoch_str, 10) catch {
                self.allocator.free(key);
                continue;
            };
            const start_offset = std.fmt.parseInt(i64, start_offset_str, 10) catch {
                self.allocator.free(key);
                continue;
            };
            const batch_count = std.fmt.parseInt(usize, batch_count_str, 10) catch {
                self.allocator.free(key);
                continue;
            };

            const batches: []ShareStateBatchEntry = if (batch_count > 0) self.allocator.alloc(ShareStateBatchEntry, batch_count) catch {
                self.allocator.free(key);
                continue;
            } else &.{};
            var valid_batches = true;
            for (batches) |*batch| {
                const first_offset_str = fields.next() orelse {
                    valid_batches = false;
                    break;
                };
                const last_offset_str = fields.next() orelse {
                    valid_batches = false;
                    break;
                };
                const delivery_state_str = fields.next() orelse {
                    valid_batches = false;
                    break;
                };
                const delivery_count_str = fields.next() orelse {
                    valid_batches = false;
                    break;
                };

                const first_offset = std.fmt.parseInt(i64, first_offset_str, 10) catch {
                    valid_batches = false;
                    break;
                };
                const last_offset = std.fmt.parseInt(i64, last_offset_str, 10) catch {
                    valid_batches = false;
                    break;
                };
                const delivery_state = std.fmt.parseInt(i8, delivery_state_str, 10) catch {
                    valid_batches = false;
                    break;
                };
                const delivery_count = std.fmt.parseInt(i16, delivery_count_str, 10) catch {
                    valid_batches = false;
                    break;
                };

                batch.* = .{
                    .first_offset = first_offset,
                    .last_offset = last_offset,
                    .delivery_state = delivery_state,
                    .delivery_count = delivery_count,
                };
            }
            if (!valid_batches) {
                self.allocator.free(key);
                if (batches.len > 0) self.allocator.free(batches);
                continue;
            }

            try entries.append(.{
                .key = key,
                .state_epoch = state_epoch,
                .start_offset = start_offset,
                .batches = batches,
            });
        }

        log.info("Loaded {d} share group state entries from share_group_states.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    pub fn freeShareGroupStateEntries(self: *MetadataPersistence, entries: []ShareGroupStateEntry) void {
        for (entries) |entry| {
            self.allocator.free(entry.key);
            if (entry.batches.len > 0) self.allocator.free(entry.batches);
        }
        if (entries.len > 0) self.allocator.free(entries);
    }

    /// Save local share fetch-session epochs to disk.
    /// Format: share_group_sessions.meta TSV with a hex-encoded internal session key.
    pub fn saveShareGroupSessions(self: *MetadataPersistence, sessions: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/share_group_sessions.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        var it = sessions.iterator();
        while (it.next()) |entry| {
            try file.writeAll("share_session\t");
            try writeHex(file, entry.key_ptr.*);
            try writer.print("\t{d}\n", .{entry.value_ptr.*});
        }
        try file.sync();
    }

    pub fn loadShareGroupSessions(self: *MetadataPersistence) ![]ShareGroupSessionEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/share_group_sessions.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No share_group_sessions.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read share_group_sessions.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(ShareGroupSessionEntry).init(self.allocator);
        errdefer {
            for (entries.items) |entry| self.allocator.free(entry.key);
            entries.deinit();
        }

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;
            if (!std.mem.eql(u8, tag, "share_session")) continue;

            const key_hex = fields.next() orelse continue;
            const epoch_str = fields.next() orelse continue;

            const key = decodeHexAlloc(self.allocator, key_hex) catch continue;
            const epoch = std.fmt.parseInt(i32, epoch_str, 10) catch {
                self.allocator.free(key);
                continue;
            };

            try entries.append(.{
                .key = key,
                .epoch = epoch,
            });
        }

        log.info("Loaded {d} share group session entries from share_group_sessions.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    pub fn freeShareGroupSessionEntries(self: *MetadataPersistence, entries: []ShareGroupSessionEntry) void {
        for (entries) |entry| self.allocator.free(entry.key);
        if (entries.len > 0) self.allocator.free(entries);
    }

    /// Save local finalized feature metadata to disk.
    /// Format: finalized_features.meta TSV with an epoch row and hex feature names.
    pub fn saveFinalizedFeatures(self: *MetadataPersistence, features: anytype, epoch: i64) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/finalized_features.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        try writer.print("epoch\t{d}\n", .{epoch});
        var it = features.iterator();
        while (it.next()) |entry| {
            try file.writeAll("feature\t");
            try writeHex(file, entry.key_ptr.*);
            try writer.print("\t{d}\n", .{entry.value_ptr.*});
        }
        try file.sync();
    }

    pub fn loadFinalizedFeatures(self: *MetadataPersistence) !FinalizedFeatureSnapshot {
        const dir = self.data_dir orelse return .{ .epoch = -1, .features = &.{} };

        const path = try std.fmt.allocPrint(self.allocator, "{s}/finalized_features.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No finalized_features.meta found: {}", .{err});
            return .{ .epoch = -1, .features = &.{} };
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read finalized_features.meta: {}", .{err});
            return .{ .epoch = -1, .features = &.{} };
        };
        defer self.allocator.free(content);

        var epoch: i64 = -1;
        var entries = std.array_list.Managed(FinalizedFeatureEntry).init(self.allocator);
        errdefer {
            for (entries.items) |entry| self.allocator.free(entry.name);
            entries.deinit();
        }

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;
            if (std.mem.eql(u8, tag, "epoch")) {
                const epoch_str = fields.next() orelse continue;
                epoch = std.fmt.parseInt(i64, epoch_str, 10) catch epoch;
                continue;
            }
            if (!std.mem.eql(u8, tag, "feature")) continue;

            const name_hex = fields.next() orelse continue;
            const max_version_str = fields.next() orelse continue;
            const name = decodeHexAlloc(self.allocator, name_hex) catch continue;
            const max_version_level = std.fmt.parseInt(i16, max_version_str, 10) catch {
                self.allocator.free(name);
                continue;
            };
            if (name.len == 0 or max_version_level < 1) {
                self.allocator.free(name);
                continue;
            }

            try entries.append(.{
                .name = name,
                .max_version_level = max_version_level,
            });
        }

        log.info("Loaded {d} finalized feature entries from finalized_features.meta", .{entries.items.len});
        return .{
            .epoch = if (entries.items.len > 0 and epoch < 0) 0 else epoch,
            .features = try entries.toOwnedSlice(),
        };
    }

    pub fn freeFinalizedFeatureSnapshot(self: *MetadataPersistence, snapshot: FinalizedFeatureSnapshot) void {
        for (snapshot.features) |entry| self.allocator.free(entry.name);
        if (snapshot.features.len > 0) self.allocator.free(snapshot.features);
    }

    /// Save ACL entries to disk.
    /// Format: principal\tresource_type\tresource_name\tpattern_type\toperation\tpermission\thost
    pub fn saveAcls(self: *MetadataPersistence, acls: []const Authorizer.AclEntry) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/acls.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        const writer = file.writer();
        for (acls) |acl| {
            try writer.print("{s}\t{d}\t{s}\t{d}\t{d}\t{d}\t{s}\n", .{
                acl.principal,
                @intFromEnum(acl.resource_type),
                acl.resource_name,
                @intFromEnum(acl.pattern_type),
                @intFromEnum(acl.operation),
                @intFromEnum(acl.permission),
                acl.host,
            });
        }
        try file.sync();
    }

    pub const AclEntry = struct {
        principal: []u8,
        resource_type: i8,
        resource_name: []u8,
        pattern_type: i8,
        operation: i8,
        permission: i8,
        host: []u8,
    };

    /// Load ACL entries from disk.
    pub fn loadAcls(self: *MetadataPersistence) ![]AclEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/acls.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No acls.meta found: {}", .{err});
            return &.{};
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read acls.meta: {}", .{err});
            return &.{};
        };
        defer self.allocator.free(content);

        var entries = std.array_list.Managed(AclEntry).init(self.allocator);

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            var fields = std.mem.splitSequence(u8, line, "\t");
            const principal = fields.next() orelse continue;
            const rt_str = fields.next() orelse continue;
            const rn = fields.next() orelse continue;
            const pt_str = fields.next() orelse continue;
            const op_str = fields.next() orelse continue;
            const perm_str = fields.next() orelse continue;
            const host = fields.next() orelse continue;

            try entries.append(.{
                .principal = try self.allocator.dupe(u8, principal),
                .resource_type = std.fmt.parseInt(i8, rt_str, 10) catch continue,
                .resource_name = try self.allocator.dupe(u8, rn),
                .pattern_type = std.fmt.parseInt(i8, pt_str, 10) catch continue,
                .operation = std.fmt.parseInt(i8, op_str, 10) catch continue,
                .permission = std.fmt.parseInt(i8, perm_str, 10) catch continue,
                .host = try self.allocator.dupe(u8, host),
            });
        }

        log.info("Loaded {d} ACLs from acls.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    /// Save local AutoMQ controller-style metadata to disk.
    /// Format is line-oriented TSV. Binary/string fields are hex encoded so tabs
    /// and newlines in metadata payloads do not corrupt parsing.
    pub fn saveAutoMqMetadata(
        self: *MetadataPersistence,
        kvs: anytype,
        nodes: anytype,
        next_node_id: i32,
        license: ?[]const u8,
        zone_router_metadata: ?[]const u8,
        zone_router_epoch: i64,
        group_promotions: anytype,
    ) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/automq.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        try writer.print("version\t1\n", .{});
        try writer.print("next_node_id\t{d}\n", .{next_node_id});
        try writer.print("zone_router_epoch\t{d}\n", .{zone_router_epoch});
        if (license) |value| {
            try file.writeAll("license\t");
            try writeHex(file, value);
            try file.writeAll("\n");
        }
        if (zone_router_metadata) |value| {
            try file.writeAll("zone_router_metadata\t");
            try writeHex(file, value);
            try file.writeAll("\n");
        }

        var kv_it = kvs.iterator();
        while (kv_it.next()) |entry| {
            try file.writeAll("kv\t");
            try writeHex(file, entry.key_ptr.*);
            try file.writeAll("\t");
            try writeHex(file, entry.value_ptr.*);
            try file.writeAll("\n");
        }

        var node_it = nodes.iterator();
        while (node_it.next()) |entry| {
            try writer.print("node\t{d}\t{d}\t", .{ entry.key_ptr.*, entry.value_ptr.node_epoch });
            try writeHex(file, entry.value_ptr.wal_config);
            try file.writeAll("\n");
        }

        var group_it = group_promotions.iterator();
        while (group_it.next()) |entry| {
            try file.writeAll("group\t");
            try writeHex(file, entry.key_ptr.*);
            try file.writeAll("\t");
            try writeHex(file, entry.value_ptr.link_id);
            try writer.print("\t{d}\n", .{@intFromBool(entry.value_ptr.promoted)});
        }
        try file.sync();
    }

    /// Load local AutoMQ controller-style metadata from disk.
    /// Returns an empty/default snapshot if no data directory or metadata file exists.
    pub fn loadAutoMqMetadata(self: *MetadataPersistence) !AutoMqMetadataSnapshot {
        const dir = self.data_dir orelse return emptyAutoMqMetadataSnapshot();

        const path = try std.fmt.allocPrint(self.allocator, "{s}/automq.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No automq.meta found: {}", .{err});
            return emptyAutoMqMetadataSnapshot();
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 4 * 1024 * 1024) catch |err| {
            log.warn("Failed to read automq.meta: {}", .{err});
            return emptyAutoMqMetadataSnapshot();
        };
        defer self.allocator.free(content);

        var next_node_id: i32 = 1;
        var zone_router_epoch: i64 = 0;
        var license: ?[]u8 = null;
        var zone_router_metadata: ?[]u8 = null;
        var kvs = std.array_list.Managed(AutoMqKvEntry).init(self.allocator);
        var nodes = std.array_list.Managed(AutoMqNodeEntry).init(self.allocator);
        var group_promotions = std.array_list.Managed(AutoMqGroupPromotionEntry).init(self.allocator);
        defer kvs.deinit();
        defer nodes.deinit();
        defer group_promotions.deinit();
        errdefer {
            if (license) |value| self.allocator.free(value);
            if (zone_router_metadata) |value| self.allocator.free(value);
            for (kvs.items) |entry| {
                self.allocator.free(entry.key);
                self.allocator.free(entry.value);
            }
            for (nodes.items) |entry| self.allocator.free(entry.wal_config);
            for (group_promotions.items) |entry| {
                self.allocator.free(entry.group_id);
                self.allocator.free(entry.link_id);
            }
        }

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const tag = fields.next() orelse continue;

            if (std.mem.eql(u8, tag, "version")) {
                continue;
            } else if (std.mem.eql(u8, tag, "next_node_id")) {
                const value = fields.next() orelse continue;
                next_node_id = std.fmt.parseInt(i32, value, 10) catch next_node_id;
            } else if (std.mem.eql(u8, tag, "zone_router_epoch")) {
                const value = fields.next() orelse continue;
                zone_router_epoch = std.fmt.parseInt(i64, value, 10) catch zone_router_epoch;
            } else if (std.mem.eql(u8, tag, "license")) {
                const encoded = fields.next() orelse continue;
                const decoded = decodeHexAlloc(self.allocator, encoded) catch continue;
                if (license) |old| self.allocator.free(old);
                license = decoded;
            } else if (std.mem.eql(u8, tag, "zone_router_metadata")) {
                const encoded = fields.next() orelse continue;
                const decoded = decodeHexAlloc(self.allocator, encoded) catch continue;
                if (zone_router_metadata) |old| self.allocator.free(old);
                zone_router_metadata = decoded;
            } else if (std.mem.eql(u8, tag, "kv")) {
                const key_hex = fields.next() orelse continue;
                const value_hex = fields.next() orelse continue;
                const key = decodeHexAlloc(self.allocator, key_hex) catch continue;
                const value = decodeHexAlloc(self.allocator, value_hex) catch {
                    self.allocator.free(key);
                    continue;
                };
                kvs.append(.{ .key = key, .value = value }) catch |err| {
                    self.allocator.free(key);
                    self.allocator.free(value);
                    return err;
                };
            } else if (std.mem.eql(u8, tag, "node")) {
                const node_id_str = fields.next() orelse continue;
                const node_epoch_str = fields.next() orelse continue;
                const wal_config_hex = fields.next() orelse continue;
                const node_id = std.fmt.parseInt(i32, node_id_str, 10) catch continue;
                const node_epoch = std.fmt.parseInt(i64, node_epoch_str, 10) catch continue;
                const wal_config = decodeHexAlloc(self.allocator, wal_config_hex) catch continue;
                nodes.append(.{ .node_id = node_id, .node_epoch = node_epoch, .wal_config = wal_config }) catch |err| {
                    self.allocator.free(wal_config);
                    return err;
                };
            } else if (std.mem.eql(u8, tag, "group")) {
                const group_id_hex = fields.next() orelse continue;
                const link_id_hex = fields.next() orelse continue;
                const promoted_str = fields.next() orelse continue;
                const promoted_int = std.fmt.parseInt(u8, promoted_str, 10) catch continue;
                const group_id = decodeHexAlloc(self.allocator, group_id_hex) catch continue;
                const link_id = decodeHexAlloc(self.allocator, link_id_hex) catch {
                    self.allocator.free(group_id);
                    continue;
                };
                group_promotions.append(.{ .group_id = group_id, .link_id = link_id, .promoted = promoted_int != 0 }) catch |err| {
                    self.allocator.free(group_id);
                    self.allocator.free(link_id);
                    return err;
                };
            }
        }

        const kv_slice = try kvs.toOwnedSlice();
        errdefer freeAutoMqKvEntries(self.allocator, kv_slice);
        const node_slice = try nodes.toOwnedSlice();
        errdefer freeAutoMqNodeEntries(self.allocator, node_slice);
        const group_slice = try group_promotions.toOwnedSlice();

        log.info("Loaded AutoMQ metadata from automq.meta (kvs={d}, nodes={d}, groups={d}, next_node_id={d})", .{ kv_slice.len, node_slice.len, group_slice.len, next_node_id });
        return .{
            .next_node_id = next_node_id,
            .zone_router_epoch = zone_router_epoch,
            .license = license,
            .zone_router_metadata = zone_router_metadata,
            .kvs = kv_slice,
            .nodes = node_slice,
            .group_promotions = group_slice,
        };
    }

    /// Save a binary ObjectManager snapshot to disk.
    pub fn saveObjectManagerSnapshot(self: *MetadataPersistence, snapshot: []const u8) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/objects.snapshot", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(snapshot);
        try file.sync();
    }

    /// Load a binary ObjectManager snapshot from disk.
    /// Returns null if no data directory or snapshot file exists.
    pub fn loadObjectManagerSnapshot(self: *MetadataPersistence) !?[]u8 {
        const dir = self.data_dir orelse return null;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/objects.snapshot", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No objects.snapshot found: {}", .{err});
            return null;
        };
        defer file.close();

        return file.readToEndAlloc(self.allocator, 64 * 1024 * 1024) catch |err| {
            log.warn("Failed to read objects.snapshot: {}", .{err});
            return null;
        };
    }

    /// Save the PreparedObjectRegistry binary snapshot to disk.
    pub fn savePreparedObjectRegistrySnapshot(self: *MetadataPersistence, snapshot: []const u8) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/prepared.snapshot", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(snapshot);
        try file.sync();
    }

    /// Load the PreparedObjectRegistry binary snapshot from disk.
    /// Returns null if no data directory or snapshot file exists.
    pub fn loadPreparedObjectRegistrySnapshot(self: *MetadataPersistence) !?[]u8 {
        const dir = self.data_dir orelse return null;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/prepared.snapshot", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch |err| {
            log.debug("No prepared.snapshot found: {}", .{err});
            return null;
        };
        defer file.close();

        return file.readToEndAlloc(self.allocator, 1024 * 1024) catch |err| {
            log.warn("Failed to read prepared.snapshot: {}", .{err});
            return null;
        };
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "MetadataPersistence save and load topics" {
    const tmp_dir = "/tmp/automq-meta-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    // Create a mock topics map
    var topics = std.StringHashMap(struct { name: []const u8, num_partitions: i32, replication_factor: i16 }).init(testing.allocator);
    defer topics.deinit();

    try topics.put("topic-a", .{ .name = "topic-a", .num_partitions = 3, .replication_factor = 1 });
    try topics.put("topic-b", .{ .name = "topic-b", .num_partitions = 1, .replication_factor = 2 });

    try persistence.saveTopics(&topics);

    const loaded = try persistence.loadTopics();
    defer {
        for (loaded) |e| testing.allocator.free(e.name);
        testing.allocator.free(loaded);
    }

    try testing.expectEqual(@as(usize, 2), loaded.len);

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "MetadataPersistence save and load topic configs round-trip" {
    const tmp_dir = "/tmp/automq-topic-config-meta-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const TopicConfig = struct {
        retention_ms: i64,
        retention_bytes: i64,
        max_message_bytes: i32,
        segment_bytes: i64,
        cleanup_policy: []const u8,
        compression_type: []const u8,
        min_insync_replicas: i32,
    };
    const Topic = struct {
        name: []const u8,
        num_partitions: i32,
        replication_factor: i16,
        topic_id: [16]u8,
        config: TopicConfig,
    };
    var topics = std.StringHashMap(Topic).init(testing.allocator);
    defer topics.deinit();

    const topic_id = [_]u8{ 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x4d, 0xef, 0x80, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd };
    try topics.put("topic\tconfigured", .{
        .name = "topic\tconfigured",
        .num_partitions = 6,
        .replication_factor = 3,
        .topic_id = topic_id,
        .config = .{
            .retention_ms = 1234,
            .retention_bytes = 5678,
            .max_message_bytes = 9000,
            .segment_bytes = 131072,
            .cleanup_policy = "compact,delete",
            .compression_type = "lz4",
            .min_insync_replicas = 2,
        },
    });

    try persistence.saveTopics(&topics);

    const loaded = try persistence.loadTopics();
    defer {
        for (loaded) |e| testing.allocator.free(e.name);
        testing.allocator.free(loaded);
    }

    try testing.expectEqual(@as(usize, 1), loaded.len);
    try testing.expectEqualStrings("topic\tconfigured", loaded[0].name);
    try testing.expectEqual(@as(i32, 6), loaded[0].num_partitions);
    try testing.expectEqual(@as(i16, 3), loaded[0].replication_factor);
    try testing.expectEqualSlices(u8, &topic_id, &loaded[0].topic_id);
    try testing.expectEqual(@as(i64, 1234), loaded[0].retention_ms);
    try testing.expectEqual(@as(i64, 5678), loaded[0].retention_bytes);
    try testing.expectEqual(@as(i32, 9000), loaded[0].max_message_bytes);
    try testing.expectEqual(@as(i64, 131072), loaded[0].segment_bytes);
    try testing.expectEqualStrings("compact,delete", loaded[0].cleanup_policy);
    try testing.expectEqualStrings("lz4", loaded[0].compression_type);
    try testing.expectEqual(@as(i32, 2), loaded[0].min_insync_replicas);
}

test "MetadataPersistence no data dir" {
    var persistence = MetadataPersistence.init(testing.allocator, null);
    const loaded = try persistence.loadTopics();
    try testing.expectEqual(@as(usize, 0), loaded.len);
}

test "MetadataPersistence save and load transactions round-trip" {
    const tmp_dir = "/tmp/automq-txn-persist-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    // Use the real TransactionCoordinator for a faithful round-trip test
    const TxnCoord = @import("txn_coordinator.zig").TransactionCoordinator;
    var coord = TxnCoord.init(testing.allocator);
    defer coord.deinit();

    // Create two transactions: one transactional, one non-transactional
    const r1 = try coord.initProducerId("persist-txn");
    _ = try coord.addPartitionsToTxn(r1.producer_id, r1.producer_epoch, "topic-a", 0);
    _ = try coord.initProducerId(null);

    try persistence.saveTransactions(&coord);

    const snapshot = try persistence.loadTransactions();
    defer persistence.freeTransactionSnapshot(snapshot);

    try testing.expectEqual(coord.next_producer_id, snapshot.next_producer_id);
    try testing.expectEqual(@as(usize, 2), snapshot.entries.len);

    // Verify at least one entry has the transactional_id we set
    var found_tid = false;
    for (snapshot.entries) |e| {
        if (e.transactional_id) |tid| {
            if (std.mem.eql(u8, tid, "persist-txn")) {
                found_tid = true;
                try testing.expectEqual(@as(i16, 0), e.producer_epoch);
                try testing.expectEqual(@as(usize, 1), e.partitions.len);
                try testing.expectEqualStrings("topic-a", e.partitions[0].topic);
                try testing.expectEqual(@as(i32, 0), e.partitions[0].partition);
            }
        }
    }
    try testing.expect(found_tid);
}

test "MetadataPersistence load transactions missing file" {
    const tmp_dir = "/tmp/automq-txn-missing-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const snapshot = try persistence.loadTransactions();
    defer persistence.freeTransactionSnapshot(snapshot);
    try testing.expectEqual(@as(i64, 1000), snapshot.next_producer_id);
    try testing.expectEqual(@as(usize, 0), snapshot.entries.len);
}

test "MetadataPersistence save and load producer sequences round-trip" {
    const tmp_dir = "/tmp/automq-seq-persist-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    // Create a mock sequences map: partition_key → {producer_id, last_sequence, producer_epoch}
    const SeqInfo = struct {
        producer_id: i64,
        last_sequence: i32,
        producer_epoch: i16,
    };
    var sequences = std.AutoHashMap(u64, SeqInfo).init(testing.allocator);
    defer sequences.deinit();

    try sequences.put(100, .{ .producer_id = 2000, .last_sequence = 42, .producer_epoch = 1 });
    try sequences.put(200, .{ .producer_id = 2001, .last_sequence = 99, .producer_epoch = 0 });

    try persistence.saveProducerSequences(&sequences);

    const loaded = try persistence.loadProducerSequences();
    defer testing.allocator.free(loaded);

    try testing.expectEqual(@as(usize, 2), loaded.len);

    // Verify entries contain expected data (order may vary due to hash map)
    var found_100 = false;
    var found_200 = false;
    for (loaded) |e| {
        if (e.partition_key == 100) {
            found_100 = true;
            try testing.expectEqual(@as(i64, 2000), e.producer_id);
            try testing.expectEqual(@as(i32, 42), e.last_sequence);
            try testing.expectEqual(@as(i16, 1), e.producer_epoch);
        }
        if (e.partition_key == 200) {
            found_200 = true;
            try testing.expectEqual(@as(i64, 2001), e.producer_id);
            try testing.expectEqual(@as(i32, 99), e.last_sequence);
            try testing.expectEqual(@as(i16, 0), e.producer_epoch);
        }
    }
    try testing.expect(found_100);
    try testing.expect(found_200);
}

test "MetadataPersistence load producer sequences missing file" {
    const tmp_dir = "/tmp/automq-seq-missing-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const loaded = try persistence.loadProducerSequences();
    try testing.expectEqual(@as(usize, 0), loaded.len);
}

test "MetadataPersistence save and load partition states round-trip" {
    const tmp_dir = "/tmp/automq-partition-state-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const PartitionState = struct {
        topic: []const u8,
        partition_id: i32,
        next_offset: u64,
        log_start_offset: u64,
        high_watermark: u64,
        last_stable_offset: u64,
        first_unstable_txn_offset: ?u64,
    };
    var partitions = std.StringHashMap(PartitionState).init(testing.allocator);
    defer partitions.deinit();

    try partitions.put("topic\tA-0", .{
        .topic = "topic\tA",
        .partition_id = 0,
        .next_offset = 12,
        .log_start_offset = 3,
        .high_watermark = 10,
        .last_stable_offset = 8,
        .first_unstable_txn_offset = 8,
    });
    try partitions.put("topic-B-1", .{
        .topic = "topic-B",
        .partition_id = 1,
        .next_offset = 7,
        .log_start_offset = 0,
        .high_watermark = 7,
        .last_stable_offset = 7,
        .first_unstable_txn_offset = null,
    });

    try persistence.savePartitionStates(&partitions);

    const loaded = try persistence.loadPartitionStates();
    defer {
        for (loaded) |entry| testing.allocator.free(entry.topic);
        testing.allocator.free(loaded);
    }

    try testing.expectEqual(@as(usize, 2), loaded.len);
    var found_a = false;
    var found_b = false;
    for (loaded) |entry| {
        if (std.mem.eql(u8, entry.topic, "topic\tA")) {
            found_a = true;
            try testing.expectEqual(@as(i32, 0), entry.partition_id);
            try testing.expectEqual(@as(u64, 12), entry.next_offset);
            try testing.expectEqual(@as(u64, 3), entry.log_start_offset);
            try testing.expectEqual(@as(u64, 10), entry.high_watermark);
            try testing.expectEqual(@as(u64, 8), entry.last_stable_offset);
            try testing.expectEqual(@as(u64, 8), entry.first_unstable_txn_offset.?);
        } else if (std.mem.eql(u8, entry.topic, "topic-B")) {
            found_b = true;
            try testing.expectEqual(@as(i32, 1), entry.partition_id);
            try testing.expect(entry.first_unstable_txn_offset == null);
        }
    }
    try testing.expect(found_a);
    try testing.expect(found_b);
}

test "MetadataPersistence load partition states missing file" {
    const tmp_dir = "/tmp/automq-partition-state-missing-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const loaded = try persistence.loadPartitionStates();
    try testing.expectEqual(@as(usize, 0), loaded.len);
}

test "MetadataPersistence save and load partition reassignments round-trip" {
    const tmp_dir = "/tmp/automq-partition-reassignment-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const PartitionReassignment = struct {
        topic: []const u8,
        partition_index: i32,
        replicas: []const i32,
        adding_replicas: []const i32,
        removing_replicas: []const i32,
    };
    var reassignments = std.StringHashMap(PartitionReassignment).init(testing.allocator);
    defer reassignments.deinit();

    const replicas_a = [_]i32{ 2, 3 };
    const adding_a = [_]i32{ 2, 3 };
    const removing_a = [_]i32{1};
    const replicas_b = [_]i32{4};
    const adding_b = [_]i32{4};
    try reassignments.put("topic\tA-0", .{
        .topic = "topic\tA",
        .partition_index = 0,
        .replicas = &replicas_a,
        .adding_replicas = &adding_a,
        .removing_replicas = &removing_a,
    });
    try reassignments.put("topic-B-1", .{
        .topic = "topic-B",
        .partition_index = 1,
        .replicas = &replicas_b,
        .adding_replicas = &adding_b,
        .removing_replicas = &.{},
    });

    try persistence.savePartitionReassignments(&reassignments);

    const loaded = try persistence.loadPartitionReassignments();
    defer persistence.freePartitionReassignmentEntries(loaded);

    try testing.expectEqual(@as(usize, 2), loaded.len);
    var found_a = false;
    var found_b = false;
    for (loaded) |entry| {
        if (std.mem.eql(u8, entry.topic, "topic\tA")) {
            found_a = true;
            try testing.expectEqual(@as(i32, 0), entry.partition_index);
            try testing.expectEqualSlices(i32, &replicas_a, entry.replicas);
            try testing.expectEqualSlices(i32, &adding_a, entry.adding_replicas);
            try testing.expectEqualSlices(i32, &removing_a, entry.removing_replicas);
        } else if (std.mem.eql(u8, entry.topic, "topic-B")) {
            found_b = true;
            try testing.expectEqual(@as(i32, 1), entry.partition_index);
            try testing.expectEqualSlices(i32, &replicas_b, entry.replicas);
            try testing.expectEqualSlices(i32, &adding_b, entry.adding_replicas);
            try testing.expectEqual(@as(usize, 0), entry.removing_replicas.len);
        }
    }
    try testing.expect(found_a);
    try testing.expect(found_b);
}

test "MetadataPersistence load partition reassignments missing file" {
    const tmp_dir = "/tmp/automq-partition-reassignment-missing-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const loaded = try persistence.loadPartitionReassignments();
    try testing.expectEqual(@as(usize, 0), loaded.len);
}

test "MetadataPersistence save and load replica directory assignments round-trip" {
    const tmp_dir = "/tmp/automq-replica-directory-assignment-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const ReplicaDirectoryAssignment = struct {
        topic_id: [16]u8,
        partition_index: i32,
        directory_id: [16]u8,
    };
    var assignments = std.StringHashMap(ReplicaDirectoryAssignment).init(testing.allocator);
    defer assignments.deinit();

    const topic_id = [_]u8{0x11} ** 16;
    const directory_id = [_]u8{0x22} ** 16;
    try assignments.put("topic-0", .{
        .topic_id = topic_id,
        .partition_index = 3,
        .directory_id = directory_id,
    });

    try persistence.saveReplicaDirectoryAssignments(&assignments);

    const loaded = try persistence.loadReplicaDirectoryAssignments();
    defer persistence.freeReplicaDirectoryAssignmentEntries(loaded);

    try testing.expectEqual(@as(usize, 1), loaded.len);
    try testing.expectEqualSlices(u8, topic_id[0..], loaded[0].topic_id[0..]);
    try testing.expectEqual(@as(i32, 3), loaded[0].partition_index);
    try testing.expectEqualSlices(u8, directory_id[0..], loaded[0].directory_id[0..]);
}

test "MetadataPersistence save and load share group states round-trip" {
    const tmp_dir = "/tmp/automq-share-group-state-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    const ShareStateBatch = struct {
        first_offset: i64,
        last_offset: i64,
        delivery_state: i8,
        delivery_count: i16,
    };
    const SharePartitionState = struct {
        state_epoch: i32,
        start_offset: i64,
        batches: []const ShareStateBatch,
    };
    var states = std.StringHashMap(SharePartitionState).init(testing.allocator);
    defer states.deinit();

    const batches = [_]ShareStateBatch{.{
        .first_offset = 5,
        .last_offset = 8,
        .delivery_state = 2,
        .delivery_count = 1,
    }};
    try states.put("group:00112233445566778899aabbccddeeff:0", .{
        .state_epoch = 4,
        .start_offset = 5,
        .batches = &batches,
    });

    try persistence.saveShareGroupStates(&states);

    const loaded = try persistence.loadShareGroupStates();
    defer persistence.freeShareGroupStateEntries(loaded);

    try testing.expectEqual(@as(usize, 1), loaded.len);
    try testing.expectEqualStrings("group:00112233445566778899aabbccddeeff:0", loaded[0].key);
    try testing.expectEqual(@as(i32, 4), loaded[0].state_epoch);
    try testing.expectEqual(@as(i64, 5), loaded[0].start_offset);
    try testing.expectEqual(@as(usize, 1), loaded[0].batches.len);
    try testing.expectEqual(@as(i64, 5), loaded[0].batches[0].first_offset);
    try testing.expectEqual(@as(i64, 8), loaded[0].batches[0].last_offset);
    try testing.expectEqual(@as(i8, 2), loaded[0].batches[0].delivery_state);
    try testing.expectEqual(@as(i16, 1), loaded[0].batches[0].delivery_count);
}

test "MetadataPersistence save and load share group sessions round-trip" {
    const tmp_dir = "/tmp/automq-share-group-session-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    var sessions = std.StringHashMap(i32).init(testing.allocator);
    defer {
        var it = sessions.iterator();
        while (it.next()) |entry| testing.allocator.free(entry.key_ptr.*);
        sessions.deinit();
    }

    const key = try testing.allocator.dupe(u8, "10:group\tname-member\nid");
    try sessions.put(key, 7);

    try persistence.saveShareGroupSessions(&sessions);

    const loaded = try persistence.loadShareGroupSessions();
    defer persistence.freeShareGroupSessionEntries(loaded);

    try testing.expectEqual(@as(usize, 1), loaded.len);
    try testing.expectEqualStrings("10:group\tname-member\nid", loaded[0].key);
    try testing.expectEqual(@as(i32, 7), loaded[0].epoch);
}

test "MetadataPersistence save and load finalized features round-trip" {
    const tmp_dir = "/tmp/automq-finalized-features-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    var features = std.StringHashMap(i16).init(testing.allocator);
    defer {
        var it = features.iterator();
        while (it.next()) |entry| testing.allocator.free(entry.key_ptr.*);
        features.deinit();
    }

    const metadata_key = try testing.allocator.dupe(u8, "metadata.version");
    try features.put(metadata_key, 1);

    try persistence.saveFinalizedFeatures(&features, 4);

    const loaded = try persistence.loadFinalizedFeatures();
    defer persistence.freeFinalizedFeatureSnapshot(loaded);

    try testing.expectEqual(@as(i64, 4), loaded.epoch);
    try testing.expectEqual(@as(usize, 1), loaded.features.len);
    try testing.expectEqualStrings("metadata.version", loaded.features[0].name);
    try testing.expectEqual(@as(i16, 1), loaded.features[0].max_version_level);
}

test "MetadataPersistence save and load AutoMQ metadata round-trip" {
    const tmp_dir = "/tmp/automq-local-metadata-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    var kvs = std.StringHashMap([]const u8).init(testing.allocator);
    defer kvs.deinit();
    try kvs.put("alpha\tkey", "beta\nvalue\x00tail");

    const Node = struct {
        node_epoch: i64,
        wal_config: []const u8,
    };
    var nodes = std.AutoHashMap(i32, Node).init(testing.allocator);
    defer nodes.deinit();
    try nodes.put(7, .{ .node_epoch = 3, .wal_config = "wal://node-7\tcfg" });

    const GroupPromotion = struct {
        link_id: []const u8,
        promoted: bool,
    };
    var groups = std.StringHashMap(GroupPromotion).init(testing.allocator);
    defer groups.deinit();
    try groups.put("group\nA", .{ .link_id = "link\tA", .promoted = true });

    try persistence.saveAutoMqMetadata(
        &kvs,
        &nodes,
        12,
        "license\npayload",
        "router\tmetadata",
        44,
        &groups,
    );

    var snapshot = try persistence.loadAutoMqMetadata();
    defer persistence.freeAutoMqMetadataSnapshot(&snapshot);

    try testing.expectEqual(@as(i32, 12), snapshot.next_node_id);
    try testing.expectEqual(@as(i64, 44), snapshot.zone_router_epoch);
    try testing.expectEqualStrings("license\npayload", snapshot.license.?);
    try testing.expectEqualStrings("router\tmetadata", snapshot.zone_router_metadata.?);
    try testing.expectEqual(@as(usize, 1), snapshot.kvs.len);
    try testing.expectEqualStrings("alpha\tkey", snapshot.kvs[0].key);
    try testing.expectEqualSlices(u8, "beta\nvalue\x00tail", snapshot.kvs[0].value);
    try testing.expectEqual(@as(usize, 1), snapshot.nodes.len);
    try testing.expectEqual(@as(i32, 7), snapshot.nodes[0].node_id);
    try testing.expectEqual(@as(i64, 3), snapshot.nodes[0].node_epoch);
    try testing.expectEqualStrings("wal://node-7\tcfg", snapshot.nodes[0].wal_config);
    try testing.expectEqual(@as(usize, 1), snapshot.group_promotions.len);
    try testing.expectEqualStrings("group\nA", snapshot.group_promotions[0].group_id);
    try testing.expectEqualStrings("link\tA", snapshot.group_promotions[0].link_id);
    try testing.expect(snapshot.group_promotions[0].promoted);
}

test "MetadataPersistence load AutoMQ metadata missing file" {
    const tmp_dir = "/tmp/automq-local-metadata-missing-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);
    var snapshot = try persistence.loadAutoMqMetadata();
    defer persistence.freeAutoMqMetadataSnapshot(&snapshot);

    try testing.expectEqual(@as(i32, 1), snapshot.next_node_id);
    try testing.expectEqual(@as(i64, 0), snapshot.zone_router_epoch);
    try testing.expect(snapshot.license == null);
    try testing.expect(snapshot.zone_router_metadata == null);
    try testing.expectEqual(@as(usize, 0), snapshot.kvs.len);
    try testing.expectEqual(@as(usize, 0), snapshot.nodes.len);
    try testing.expectEqual(@as(usize, 0), snapshot.group_promotions.len);
}

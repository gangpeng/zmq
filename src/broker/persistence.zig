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
            try writer.print("{s}\t{d}\t{d}\n", .{ info.name, info.num_partitions, info.replication_factor });
        }
    }

    /// Load topic metadata from disk. Returns list of (name, partitions, rf) tuples.
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
            const name = fields.next() orelse continue;
            const parts_str = fields.next() orelse continue;
            const rf_str = fields.next() orelse continue;

            const num_parts = std.fmt.parseInt(i32, parts_str, 10) catch continue;
            const rf = std.fmt.parseInt(i16, rf_str, 10) catch continue;

            try entries.append(.{
                .name = try self.allocator.dupe(u8, name),
                .num_partitions = num_parts,
                .replication_factor = rf,
            });
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
            try writer.print("{s}\t{d}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
        }
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
            try entries.append(.{
                .key = try self.allocator.dupe(u8, key),
                .offset = offset,
            });
        }

        log.info("Loaded {d} offsets from offsets.meta", .{entries.items.len});
        return entries.toOwnedSlice();
    }

    pub const TopicEntry = struct {
        name: []u8,
        num_partitions: i32,
        replication_factor: i16,
    };

    pub const OffsetEntry = struct {
        key: []u8,
        offset: i64,
    };

    pub const TransactionEntry = struct {
        producer_id: i64,
        producer_epoch: i16,
        status: u8,
        timeout_ms: i32,
        transactional_id: ?[]u8,
    };

    pub const TransactionSnapshot = struct {
        next_producer_id: i64,
        entries: []TransactionEntry,
    };

    pub const ProducerSequenceEntry = struct {
        producer_id: i64,
        partition_key: u64,
        last_sequence: i32,
        producer_epoch: i16,
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
            const tid_str: []const u8 = if (txn.transactional_id) |tid| tid else "";
            try writer.print("{d}\t{d}\t{d}\t{d}\t{s}\n", .{
                txn.producer_id,
                txn.producer_epoch,
                @intFromEnum(txn.status),
                txn.timeout_ms,
                tid_str,
            });
        }
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

        // Remaining lines: producer_id\tepoch\tstatus\ttimeout_ms\ttransactional_id
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const pid_str = fields.next() orelse continue;
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
    defer {
        for (snapshot.entries) |e| {
            if (e.transactional_id) |tid| testing.allocator.free(tid);
        }
        testing.allocator.free(snapshot.entries);
    }

    try testing.expectEqual(coord.next_producer_id, snapshot.next_producer_id);
    try testing.expectEqual(@as(usize, 2), snapshot.entries.len);

    // Verify at least one entry has the transactional_id we set
    var found_tid = false;
    for (snapshot.entries) |e| {
        if (e.transactional_id) |tid| {
            if (std.mem.eql(u8, tid, "persist-txn")) {
                found_tid = true;
                try testing.expectEqual(@as(i16, 0), e.producer_epoch);
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

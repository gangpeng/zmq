const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = std.fs;

/// Persists broker metadata (topics, offsets) to a JSON file in the data directory.
/// Loaded on startup, saved on changes.
pub const MetadataPersistence = struct {
    data_dir: ?[]const u8,
    allocator: Allocator,

    pub fn init(alloc: Allocator, data_dir: ?[]const u8) MetadataPersistence {
        return .{ .data_dir = data_dir, .allocator = alloc };
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

        const file = fs.openFileAbsolute(path, .{}) catch return &.{};
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch return &.{};
        defer self.allocator.free(content);

        var entries = std.ArrayList(TopicEntry).init(self.allocator);

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

        const file = fs.openFileAbsolute(path, .{}) catch return &.{};
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch return &.{};
        defer self.allocator.free(content);

        var entries = std.ArrayList(OffsetEntry).init(self.allocator);

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

        const file = fs.openFileAbsolute(path, .{}) catch return .{ .next_producer_id = 1000, .entries = &.{} };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch return .{ .next_producer_id = 1000, .entries = &.{} };
        defer self.allocator.free(content);

        var entries = std.ArrayList(TransactionEntry).init(self.allocator);
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
            try writer.print("{d}\t{d}\t{d}\t{d}\n", .{
                entry.value_ptr.producer_id,
                entry.key_ptr.*,
                entry.value_ptr.last_sequence,
                entry.value_ptr.producer_epoch,
            });
        }
    }

    /// Load producer sequence state from disk.
    /// Returns empty slice if file is missing.
    pub fn loadProducerSequences(self: *MetadataPersistence) ![]ProducerSequenceEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/producer_state.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch return &.{};
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch return &.{};
        defer self.allocator.free(content);

        var entries = std.ArrayList(ProducerSequenceEntry).init(self.allocator);

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

        return entries.toOwnedSlice();
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

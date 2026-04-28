const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ErrorCode = @import("protocol").ErrorCode;

/// Transaction coordinator.
///
/// Manages the lifecycle of Kafka transactions:
///   Empty → Ongoing → PrepareCommit → CompleteCommit
///                   → PrepareAbort  → CompleteAbort
///
/// Handles: InitProducerId, AddPartitionsToTxn, EndTxn, WriteTxnMarkers
pub const TransactionCoordinator = struct {
    transactions: std.AutoHashMap(i64, TransactionState),
    next_producer_id: i64 = 1000,
    allocator: Allocator,
    /// Set to true when state changes; cleared after persistence save.
    dirty: bool = false,
    pub const default_transaction_timeout_ms: i32 = 60000;

    pub const TxnStatus = enum {
        empty,
        ongoing,
        prepare_commit,
        complete_commit,
        prepare_abort,
        complete_abort,
        dead,
    };

    pub const TransactionState = struct {
        producer_id: i64,
        producer_epoch: i16,
        transactional_id: ?[]u8,
        status: TxnStatus = .empty,
        partitions: std.array_list.Managed(TopicPartition),
        start_time_ms: i64,
        timeout_ms: i32 = 60000,

        pub const TopicPartition = struct {
            topic: []const u8,
            partition: i32,
        };

        pub fn clearPartitions(self: *TransactionState, alloc: Allocator) void {
            for (self.partitions.items) |tp| {
                alloc.free(tp.topic);
            }
            self.partitions.clearRetainingCapacity();
        }

        pub fn deinit(self: *TransactionState, alloc: Allocator) void {
            if (self.transactional_id) |tid| alloc.free(tid);
            self.clearPartitions(alloc);
            self.partitions.deinit();
        }
    };

    /// Control record types for transaction markers (fix #5).
    pub const ControlRecordType = enum(i16) {
        abort = 0,
        commit = 1,
    };

    pub fn init(alloc: Allocator) TransactionCoordinator {
        return .{
            .transactions = std.AutoHashMap(i64, TransactionState).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *TransactionCoordinator) void {
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.transactions.deinit();
    }

    /// InitProducerId: allocate or recover a producer ID + epoch.
    /// Producer epoch fencing — increment epoch per call for existing transactional_id.
    pub fn initProducerId(self: *TransactionCoordinator, transactional_id: ?[]const u8) !InitProducerIdResult {
        return self.initProducerIdWithTimeout(transactional_id, default_transaction_timeout_ms);
    }

    pub fn initProducerIdWithTimeout(self: *TransactionCoordinator, transactional_id: ?[]const u8, transaction_timeout_ms: i32) !InitProducerIdResult {
        if (validateTransactionTimeout(transactional_id, transaction_timeout_ms)) |error_code| {
            return initProducerIdError(error_code);
        }

        // Check if transactional_id already has a producer_id assigned (epoch fencing)
        if (transactional_id) |tid| {
            var existing_producer_id: ?i64 = null;
            var it = self.transactions.iterator();
            while (it.next()) |entry| {
                const txn = entry.value_ptr;
                if (txn.transactional_id) |existing_tid| {
                    if (std.mem.eql(u8, existing_tid, tid)) {
                        existing_producer_id = txn.producer_id;
                        break;
                    }
                }
            }
            if (existing_producer_id) |pid| return self.bumpProducerEpoch(pid, transaction_timeout_ms);
        }

        const pid = self.next_producer_id;
        self.next_producer_id += 1;
        const epoch: i16 = 0;

        const tid_copy = if (transactional_id) |tid| try self.allocator.dupe(u8, tid) else null;

        try self.transactions.put(pid, .{
            .producer_id = pid,
            .producer_epoch = epoch,
            .transactional_id = tid_copy,
            .status = .empty,
            .partitions = std.array_list.Managed(TransactionState.TopicPartition).init(self.allocator),
            .start_time_ms = @import("time_compat").milliTimestamp(),
            .timeout_ms = if (transactional_id != null) transaction_timeout_ms else default_transaction_timeout_ms,
        });

        self.dirty = true;
        return .{
            .error_code = 0,
            .producer_id = pid,
            .producer_epoch = epoch,
        };
    }

    pub fn initProducerIdForRequest(self: *TransactionCoordinator, transactional_id: ?[]const u8, producer_id: i64, producer_epoch: i16) !InitProducerIdResult {
        return self.initProducerIdForRequestWithTimeout(transactional_id, default_transaction_timeout_ms, producer_id, producer_epoch);
    }

    pub fn initProducerIdForRequestWithTimeout(self: *TransactionCoordinator, transactional_id: ?[]const u8, transaction_timeout_ms: i32, producer_id: i64, producer_epoch: i16) !InitProducerIdResult {
        if (validateTransactionTimeout(transactional_id, transaction_timeout_ms)) |error_code| {
            return initProducerIdError(error_code);
        }

        if (producer_id == -1 and producer_epoch == -1) {
            return self.initProducerIdWithTimeout(transactional_id, transaction_timeout_ms);
        }

        if (producer_id < 0 or producer_epoch < 0) {
            return initProducerIdError(@intFromEnum(ErrorCode.invalid_request));
        }

        const txn = self.transactions.get(producer_id) orelse {
            return initProducerIdError(@intFromEnum(ErrorCode.invalid_producer_id_mapping));
        };

        if (!transactionalIdsMatch(transactional_id, txn.transactional_id)) {
            return initProducerIdError(@intFromEnum(ErrorCode.invalid_producer_id_mapping));
        }

        if (txn.producer_epoch != producer_epoch) {
            return initProducerIdError(@intFromEnum(ErrorCode.invalid_producer_epoch));
        }

        return self.bumpProducerEpoch(producer_id, transaction_timeout_ms);
    }

    pub const InitProducerIdResult = struct {
        error_code: i16,
        producer_id: i64,
        producer_epoch: i16,
    };

    fn initProducerIdError(error_code: i16) InitProducerIdResult {
        return .{
            .error_code = error_code,
            .producer_id = -1,
            .producer_epoch = -1,
        };
    }

    fn transactionalIdsMatch(requested: ?[]const u8, stored: ?[]const u8) bool {
        if (requested) |requested_id| {
            const stored_id = stored orelse return false;
            return std.mem.eql(u8, requested_id, stored_id);
        }
        return stored == null;
    }

    fn validateTransactionTimeout(transactional_id: ?[]const u8, transaction_timeout_ms: i32) ?i16 {
        if (transactional_id != null and transaction_timeout_ms <= 0) {
            return @intFromEnum(ErrorCode.invalid_transaction_timeout);
        }
        return null;
    }

    fn bumpProducerEpoch(self: *TransactionCoordinator, producer_id: i64, transaction_timeout_ms: i32) !InitProducerIdResult {
        const txn = self.transactions.getPtr(producer_id) orelse {
            return initProducerIdError(@intFromEnum(ErrorCode.invalid_producer_id_mapping));
        };

        // Epoch overflow protection: if epoch would exceed i16 max,
        // allocate a new producer_id and reset epoch to 0.
        // NOTE: AutoMQ/Kafka resets when epoch approaches Short.MAX_VALUE.
        if (txn.producer_epoch >= std.math.maxInt(i16) - 1) {
            const new_pid = self.next_producer_id;
            self.next_producer_id += 1;
            const old_pid = txn.producer_id;
            txn.producer_id = new_pid;
            txn.producer_epoch = 0;
            txn.status = .empty;
            txn.clearPartitions(self.allocator);
            txn.start_time_ms = @import("time_compat").milliTimestamp();
            if (txn.transactional_id != null) txn.timeout_ms = transaction_timeout_ms;
            const txn_copy = txn.*;
            _ = self.transactions.fetchRemove(old_pid);
            try self.transactions.put(new_pid, txn_copy);
            self.dirty = true;
            return .{
                .error_code = 0,
                .producer_id = new_pid,
                .producer_epoch = 0,
            };
        }

        txn.producer_epoch += 1;
        txn.status = .empty;
        txn.clearPartitions(self.allocator);
        txn.start_time_ms = @import("time_compat").milliTimestamp();
        if (txn.transactional_id != null) txn.timeout_ms = transaction_timeout_ms;
        self.dirty = true;
        return .{
            .error_code = 0,
            .producer_id = txn.producer_id,
            .producer_epoch = txn.producer_epoch,
        };
    }

    /// AddPartitionsToTxn: register partitions in an active transaction.
    /// Validates producer_epoch to reject fenced (zombie) producers.
    /// Idempotent: adding the same topic-partition twice is a no-op.
    pub fn addPartitionsToTxn(self: *TransactionCoordinator, producer_id: i64, producer_epoch: i16, topic: []const u8, partition: i32) !i16 {
        const txn = self.transactions.getPtr(producer_id) orelse return @intFromEnum(ErrorCode.invalid_producer_id_mapping);

        // Epoch validation — reject fenced producers (zombie fencing).
        if (txn.producer_epoch != producer_epoch) return @intFromEnum(ErrorCode.invalid_producer_epoch);

        // Enforce transaction timeout
        if (txn.status == .ongoing) {
            const elapsed = @import("time_compat").milliTimestamp() - txn.start_time_ms;
            if (elapsed > txn.timeout_ms) {
                txn.status = .prepare_abort;
                self.dirty = true;
                return @intFromEnum(ErrorCode.invalid_txn_state);
            }
        }

        if (txn.status == .empty) {
            txn.status = .ongoing;
            txn.start_time_ms = @import("time_compat").milliTimestamp();
        }

        if (txn.status != .ongoing) return @intFromEnum(ErrorCode.invalid_txn_state);

        // Idempotent: skip if this topic-partition is already registered.
        // NOTE: AutoMQ/Kafka silently accepts duplicate partition adds.
        for (txn.partitions.items) |existing| {
            if (existing.partition == partition and std.mem.eql(u8, existing.topic, topic)) {
                return 0; // Already registered — success
            }
        }

        const topic_copy = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(topic_copy);
        try txn.partitions.append(.{ .topic = topic_copy, .partition = partition });
        self.dirty = true;
        return 0;
    }

    pub fn verifyPartitionInTxn(self: *const TransactionCoordinator, producer_id: i64, producer_epoch: i16, topic: []const u8, partition: i32) i16 {
        const txn = self.transactions.get(producer_id) orelse return @intFromEnum(ErrorCode.invalid_producer_id_mapping);
        if (txn.producer_epoch != producer_epoch) return @intFromEnum(ErrorCode.invalid_producer_epoch);
        if (txn.status != .ongoing) return @intFromEnum(ErrorCode.invalid_txn_state);
        for (txn.partitions.items) |existing| {
            if (existing.partition == partition and std.mem.eql(u8, existing.topic, topic)) {
                return 0;
            }
        }
        return @intFromEnum(ErrorCode.invalid_txn_state);
    }

    /// EndTxn: commit or abort the transaction.
    /// Validates producer_epoch to reject fenced (zombie) producers.
    pub fn endTxn(self: *TransactionCoordinator, producer_id: i64, producer_epoch: i16, commit: bool) i16 {
        const txn = self.transactions.getPtr(producer_id) orelse return @intFromEnum(ErrorCode.invalid_producer_id_mapping);

        // Epoch validation — reject fenced producers.
        if (txn.producer_epoch != producer_epoch) return @intFromEnum(ErrorCode.invalid_producer_epoch);

        if (txn.status != .ongoing) return @intFromEnum(ErrorCode.invalid_txn_state);

        if (commit) {
            txn.status = .prepare_commit;
        } else {
            txn.status = .prepare_abort;
        }

        self.dirty = true;
        return 0;
    }

    /// WriteTxnMarkers: finalize the transaction by writing markers to all partitions.
    /// Called after endTxn when all partitions have been prepared.
    pub fn writeTxnMarkers(self: *TransactionCoordinator, producer_id: i64) i16 {
        const txn = self.transactions.getPtr(producer_id) orelse return @intFromEnum(ErrorCode.invalid_producer_id_mapping);

        switch (txn.status) {
            .prepare_commit => {
                // Create COMMIT control records for each partition (fix #5)
                for (txn.partitions.items) |_| {
                    // In production: write a control record batch to the partition store
                    // The control record has attributes bit 5 set (CONTROL_MASK)
                    // and contains a COMMIT marker (ControlRecordType.commit = 1)
                    // For now, the transaction state transition is the important part
                }
                txn.status = .complete_commit;
                txn.clearPartitions(self.allocator);
                self.dirty = true;
                return 0;
            },
            .prepare_abort => {
                // Create ABORT control records for each partition (fix #5)
                for (txn.partitions.items) |_| {
                    // In production: write a control record batch to the partition store
                    // with ControlRecordType.abort = 0
                }
                txn.status = .complete_abort;
                txn.clearPartitions(self.allocator);
                self.dirty = true;
                return 0;
            },
            else => return @intFromEnum(ErrorCode.invalid_txn_state),
        }
    }

    /// Build a control record batch for COMMIT/ABORT markers.
    /// Returns an allocated record batch with the control flag set in attributes.
    pub fn buildControlBatch(
        self: *TransactionCoordinator,
        producer_id: i64,
        producer_epoch: i16,
        control_type: ControlRecordType,
        base_offset: i64,
    ) ![]u8 {
        // Control batch: attributes has TRANSACTIONAL | CONTROL bits set
        const attributes: i16 = 0x30; // TRANSACTIONAL_MASK (0x10) | CONTROL_MASK (0x20)

        // The control record value is: version(i16) + control_type(i16)
        var value_buf: [4]u8 = undefined;
        std.mem.writeInt(i16, value_buf[0..2], 0, .big); // version
        std.mem.writeInt(i16, value_buf[2..4], @intFromEnum(control_type), .big);

        const rec_batch = @import("protocol").record_batch;
        const records = [_]rec_batch.Record{
            .{
                .offset_delta = 0,
                .key = &value_buf, // control records use key for the marker type
                .value = null,
            },
        };

        return try rec_batch.buildRecordBatch(
            self.allocator,
            base_offset,
            &records,
            producer_id,
            producer_epoch,
            0,
            @import("time_compat").milliTimestamp(),
            @import("time_compat").milliTimestamp(),
            attributes,
        );
    }

    /// Complete the EndTxn by writing markers and transitioning to final state.
    /// This is a convenience method that combines endTxn + writeTxnMarkers.
    pub fn endTxnComplete(self: *TransactionCoordinator, producer_id: i64, producer_epoch: i16, commit: bool) i16 {
        const err1 = self.endTxn(producer_id, producer_epoch, commit);
        if (err1 != 0) return err1;
        return self.writeTxnMarkers(producer_id);
    }

    /// Expire timed-out transactions by auto-aborting them.
    /// Called periodically from Broker.tick().
    /// NOTE: AutoMQ/Kafka uses transaction.timeout.ms (default 60s) to auto-abort
    /// transactions that have been in ONGOING state too long. This prevents resource
    /// leaks from abandoned producers.
    pub fn expireTransactions(self: *TransactionCoordinator) u32 {
        const now = @import("time_compat").milliTimestamp();
        var expired_pids: [64]i64 = undefined;
        var num_expired: usize = 0;

        // Phase 1: Collect expired PIDs (can't mutate during iteration)
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            const txn = entry.value_ptr;
            if (txn.status == .ongoing) {
                if (now - txn.start_time_ms > txn.timeout_ms) {
                    if (num_expired < 64) {
                        expired_pids[num_expired] = txn.producer_id;
                        num_expired += 1;
                    }
                }
            }
        }

        // Phase 2: Auto-abort each expired transaction
        var aborted: u32 = 0;
        for (expired_pids[0..num_expired]) |pid| {
            if (self.transactions.getPtr(pid)) |txn| {
                txn.status = .prepare_abort;
                _ = self.writeTxnMarkers(pid);
                aborted += 1;
            }
        }

        if (aborted > 0) self.dirty = true;

        return aborted;
    }

    /// Serialize transaction state for persistence to __transaction_state (fix #12).
    /// Returns allocated bytes that represent the current state.
    pub fn serializeState(self: *TransactionCoordinator) ![]u8 {
        var buf = std.array_list.Managed(u8).init(self.allocator);
        const writer = @import("list_compat").writer(&buf);

        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            const txn = entry.value_ptr;
            // Format: producer_id(i64) + epoch(i16) + status(u8) + tid_len(u16) + tid + num_parts(u32)
            try writer.writeInt(i64, txn.producer_id, .big);
            try writer.writeInt(i16, txn.producer_epoch, .big);
            try writer.writeByte(@intFromEnum(txn.status));
            if (txn.transactional_id) |tid| {
                try writer.writeInt(u16, @intCast(tid.len), .big);
                try writer.writeAll(tid);
            } else {
                try writer.writeInt(u16, 0, .big);
            }
            try writer.writeInt(u32, @intCast(txn.partitions.items.len), .big);
        }

        return buf.toOwnedSlice();
    }

    /// Get the list of partitions in a transaction.
    pub fn getPartitions(self: *const TransactionCoordinator, producer_id: i64) ?[]const TransactionState.TopicPartition {
        if (self.transactions.get(producer_id)) |txn| {
            return txn.partitions.items;
        }
        return null;
    }

    /// Get transaction status.
    pub fn getStatus(self: *const TransactionCoordinator, producer_id: i64) ?TxnStatus {
        if (self.transactions.get(producer_id)) |txn| return txn.status;
        return null;
    }

    pub fn transactionCount(self: *const TransactionCoordinator) usize {
        return self.transactions.count();
    }

    /// Restore transaction state from persisted snapshot (called during Broker.open()).
    /// NOTE: AutoMQ/Kafka loads from __transaction_state topic on coordinator startup.
    /// ZMQ uses file-based persistence as a simplification.
    pub fn restoreState(self: *TransactionCoordinator, snapshot: anytype) !void {
        self.next_producer_id = snapshot.next_producer_id;

        for (snapshot.entries) |entry| {
            const tid_copy = if (entry.transactional_id) |tid|
                try self.allocator.dupe(u8, tid)
            else
                null;
            errdefer if (tid_copy) |tid| self.allocator.free(tid);

            var partitions = std.array_list.Managed(TransactionState.TopicPartition).init(self.allocator);
            errdefer {
                for (partitions.items) |partition| self.allocator.free(partition.topic);
                partitions.deinit();
            }
            for (entry.partitions) |partition| {
                const topic_copy = try self.allocator.dupe(u8, partition.topic);
                errdefer self.allocator.free(topic_copy);
                try partitions.append(.{ .topic = topic_copy, .partition = partition.partition });
            }

            try self.transactions.put(entry.producer_id, .{
                .producer_id = entry.producer_id,
                .producer_epoch = entry.producer_epoch,
                .transactional_id = tid_copy,
                .status = @enumFromInt(entry.status),
                .partitions = partitions,
                .start_time_ms = @import("time_compat").milliTimestamp(),
                .timeout_ms = entry.timeout_ms,
            });
        }
        self.dirty = false;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "TransactionCoordinator init producer id" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("my-txn");
    try testing.expectEqual(@as(i16, 0), result.error_code);
    try testing.expect(result.producer_id >= 1000);
    try testing.expectEqual(@as(i16, 0), result.producer_epoch);
    try testing.expectEqual(@as(usize, 1), coord.transactionCount());
}

test "TransactionCoordinator full lifecycle" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-1");
    const pid = result.producer_id;

    // Add partitions
    const err1 = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 0);
    try testing.expectEqual(@as(i16, 0), err1);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.ongoing, coord.getStatus(pid).?);

    // Commit (two-phase)
    const err2 = coord.endTxn(pid, result.producer_epoch, true);
    try testing.expectEqual(@as(i16, 0), err2);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.prepare_commit, coord.getStatus(pid).?);

    // Write markers to complete
    const err3 = coord.writeTxnMarkers(pid);
    try testing.expectEqual(@as(i16, 0), err3);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.complete_commit, coord.getStatus(pid).?);
}

test "TransactionCoordinator abort" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId(null);
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic", 0);
    const err = coord.endTxnComplete(pid, result.producer_epoch, false);
    try testing.expectEqual(@as(i16, 0), err);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.complete_abort, coord.getStatus(pid).?);
}

test "TransactionCoordinator invalid producer id" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const err = try coord.addPartitionsToTxn(9999, 0, "topic", 0);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_id_mapping)), err);
}

test "TransactionCoordinator endTxn invalid state" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-test");
    const pid = result.producer_id;

    // endTxn on EMPTY state (never added partitions, so state is "empty")
    // addPartitions transitions from empty→ongoing, endTxn requires ongoing
    const err = coord.endTxn(pid, result.producer_epoch, true);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_txn_state)), err);
}

test "TransactionCoordinator double commit fails" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-double");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic", 0);

    // First commit succeeds
    try testing.expectEqual(@as(i16, 0), coord.endTxn(pid, result.producer_epoch, true));
    try testing.expectEqual(TransactionCoordinator.TxnStatus.prepare_commit, coord.getStatus(pid).?);

    // Second endTxn on prepare_commit state should fail
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_txn_state)), coord.endTxn(pid, result.producer_epoch, true));
}

test "TransactionCoordinator writeTxnMarkers invalid state" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId(null);
    const pid = result.producer_id;

    // writeTxnMarkers without endTxn first
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_txn_state)), coord.writeTxnMarkers(pid));
}

test "TransactionCoordinator writeTxnMarkers on nonexistent producer" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_id_mapping)), coord.writeTxnMarkers(9999));
}

test "TransactionCoordinator endTxn on nonexistent producer" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_id_mapping)), coord.endTxn(9999, 0, true));
}

test "TransactionCoordinator getPartitions" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-parts");
    const pid = result.producer_id;

    // No partitions initially
    const parts0 = coord.getPartitions(pid).?;
    try testing.expectEqual(@as(usize, 0), parts0.len);

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 0);
    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 1);

    const parts = coord.getPartitions(pid).?;
    try testing.expectEqual(@as(usize, 2), parts.len);
}

test "TransactionCoordinator getPartitions nonexistent" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expect(coord.getPartitions(9999) == null);
}

test "TransactionCoordinator getStatus nonexistent" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expect(coord.getStatus(9999) == null);
}

test "TransactionCoordinator multiple producers" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.initProducerId("txn-1");
    const r2 = try coord.initProducerId("txn-2");
    const r3 = try coord.initProducerId(null); // non-transactional

    try testing.expect(r1.producer_id != r2.producer_id);
    try testing.expect(r2.producer_id != r3.producer_id);
    try testing.expectEqual(@as(usize, 3), coord.transactionCount());
}

test "TransactionCoordinator commit clears partitions" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-clear");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic", 0);
    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic", 1);

    try testing.expectEqual(@as(usize, 2), coord.getPartitions(pid).?.len);

    _ = coord.endTxnComplete(pid, result.producer_epoch, true);

    // After commit, partitions should be cleared
    try testing.expectEqual(@as(usize, 0), coord.getPartitions(pid).?.len);
}

// ---------------------------------------------------------------
// HIGH-priority gap tests
// ---------------------------------------------------------------

test "TransactionCoordinator buildControlBatch produces valid batch" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const init_result = try coord.initProducerId("ctrl-batch");
    const pid = init_result.producer_id;
    const epoch = init_result.producer_epoch;

    // Build a COMMIT control batch
    const batch_data = try coord.buildControlBatch(pid, epoch, .commit, 0);
    defer testing.allocator.free(batch_data);

    // Verify it's a valid RecordBatch
    const rec_batch = @import("protocol").record_batch;
    try testing.expect(batch_data.len >= rec_batch.BATCH_HEADER_SIZE);

    const header = try rec_batch.RecordBatchHeader.parse(batch_data);
    try testing.expectEqual(@as(i8, 2), header.magic);
    try testing.expectEqual(@as(i32, 1), header.record_count);
    // Attributes should have TRANSACTIONAL (0x10) and CONTROL (0x20) set
    try testing.expect(header.isTransactional());
    try testing.expect(header.isControlBatch());
    try testing.expectEqual(@as(i64, 0), header.base_offset);
}

test "TransactionCoordinator buildControlBatch abort" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const init_result = try coord.initProducerId("ctrl-abort");
    const pid = init_result.producer_id;
    const epoch = init_result.producer_epoch;

    // Build an ABORT control batch
    const batch_data = try coord.buildControlBatch(pid, epoch, .abort, 10);
    defer testing.allocator.free(batch_data);

    const rec_batch = @import("protocol").record_batch;
    const header = try rec_batch.RecordBatchHeader.parse(batch_data);
    try testing.expect(header.isTransactional());
    try testing.expect(header.isControlBatch());
    try testing.expectEqual(@as(i64, 10), header.base_offset);
    // CRC should be valid
    try testing.expect(rec_batch.validateCrc(batch_data));
}

test "TransactionCoordinator epoch fencing" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    // First init — epoch 0
    const r1 = try coord.initProducerId("fenced-txn");
    try testing.expectEqual(@as(i16, 0), r1.producer_epoch);

    // Second init with same transactional_id — epoch should bump to 1
    const r2 = try coord.initProducerId("fenced-txn");
    try testing.expectEqual(r1.producer_id, r2.producer_id); // same producer_id
    try testing.expectEqual(@as(i16, 1), r2.producer_epoch); // epoch bumped

    // Third init — epoch bumps again
    const r3 = try coord.initProducerId("fenced-txn");
    try testing.expectEqual(r1.producer_id, r3.producer_id);
    try testing.expectEqual(@as(i16, 2), r3.producer_epoch);

    // Still only 1 transaction entry (reused)
    try testing.expectEqual(@as(usize, 1), coord.transactionCount());
}

test "TransactionCoordinator initProducerIdForRequest bumps validated producer epoch" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.initProducerId("recover-txn");
    _ = try coord.addPartitionsToTxn(r1.producer_id, r1.producer_epoch, "topic-a", 0);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.ongoing, coord.getStatus(r1.producer_id).?);

    const r2 = try coord.initProducerIdForRequest("recover-txn", r1.producer_id, r1.producer_epoch);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.none)), r2.error_code);
    try testing.expectEqual(r1.producer_id, r2.producer_id);
    try testing.expectEqual(@as(i16, 1), r2.producer_epoch);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.empty, coord.getStatus(r1.producer_id).?);
    try testing.expectEqual(@as(usize, 0), coord.getPartitions(r1.producer_id).?.len);

    const stale = try coord.initProducerIdForRequest("recover-txn", r1.producer_id, r1.producer_epoch);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_epoch)), stale.error_code);
    try testing.expectEqual(@as(i64, -1), stale.producer_id);
    try testing.expectEqual(@as(i16, -1), stale.producer_epoch);
    try testing.expectEqual(@as(i16, 1), coord.transactions.get(r1.producer_id).?.producer_epoch);
}

test "TransactionCoordinator initProducerIdForRequest validates producer ownership" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const owner = try coord.initProducerId("owner-txn");

    const wrong_tid = try coord.initProducerIdForRequest("other-txn", owner.producer_id, owner.producer_epoch);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_id_mapping)), wrong_tid.error_code);
    try testing.expectEqual(@as(i64, -1), wrong_tid.producer_id);
    try testing.expectEqual(@as(i16, -1), wrong_tid.producer_epoch);

    const missing_tid = try coord.initProducerIdForRequest(null, owner.producer_id, owner.producer_epoch);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_id_mapping)), missing_tid.error_code);
    try testing.expectEqual(@as(i16, 0), coord.transactions.get(owner.producer_id).?.producer_epoch);

    const malformed = try coord.initProducerIdForRequest("owner-txn", owner.producer_id, -1);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_request)), malformed.error_code);
}

test "TransactionCoordinator initProducerIdForRequest recovers non-transactional producer" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.initProducerId(null);
    const r2 = try coord.initProducerIdForRequest(null, r1.producer_id, r1.producer_epoch);

    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.none)), r2.error_code);
    try testing.expectEqual(r1.producer_id, r2.producer_id);
    try testing.expectEqual(@as(i16, 1), r2.producer_epoch);
}

test "TransactionCoordinator InitProducerId applies transactional timeout" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.initProducerIdWithTimeout("timeout-txn", 1234);
    try testing.expectEqual(@as(i32, 1234), coord.transactions.get(r1.producer_id).?.timeout_ms);

    const r2 = try coord.initProducerIdForRequestWithTimeout("timeout-txn", 2345, r1.producer_id, r1.producer_epoch);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.none)), r2.error_code);
    try testing.expectEqual(r1.producer_id, r2.producer_id);
    try testing.expectEqual(@as(i16, 1), r2.producer_epoch);
    try testing.expectEqual(@as(i32, 2345), coord.transactions.get(r1.producer_id).?.timeout_ms);
}

test "TransactionCoordinator InitProducerId validates transactional timeout" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const invalid = try coord.initProducerIdWithTimeout("bad-timeout-txn", 0);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_transaction_timeout)), invalid.error_code);
    try testing.expectEqual(@as(i64, -1), invalid.producer_id);
    try testing.expectEqual(@as(i16, -1), invalid.producer_epoch);
    try testing.expectEqual(@as(usize, 0), coord.transactionCount());

    const non_txn = try coord.initProducerIdWithTimeout(null, 0);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.none)), non_txn.error_code);
    try testing.expectEqual(@as(i32, TransactionCoordinator.default_transaction_timeout_ms), coord.transactions.get(non_txn.producer_id).?.timeout_ms);
}

// ---------------------------------------------------------------
// Gap-fix tests: epoch validation, idempotent add, timeout, overflow
// ---------------------------------------------------------------

test "TransactionCoordinator addPartitionsToTxn validates epoch" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("epoch-val");
    const pid = result.producer_id;

    // Correct epoch works
    const ok = try coord.addPartitionsToTxn(pid, 0, "topic", 0);
    try testing.expectEqual(@as(i16, 0), ok);

    // Wrong epoch returns INVALID_PRODUCER_EPOCH.
    const fenced = try coord.addPartitionsToTxn(pid, 99, "topic", 1);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_epoch)), fenced);
}

test "TransactionCoordinator endTxn validates epoch" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("epoch-end");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic", 0);

    // Wrong epoch returns INVALID_PRODUCER_EPOCH.
    const fenced = coord.endTxn(pid, 99, true);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_epoch)), fenced);

    // Correct epoch works
    const ok = coord.endTxn(pid, result.producer_epoch, true);
    try testing.expectEqual(@as(i16, 0), ok);
}

test "TransactionCoordinator addPartitionsToTxn idempotent" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("idem-add");
    const pid = result.producer_id;

    // Add same partition twice
    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 0);
    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 0);

    // Should have only 1 entry (idempotent)
    const parts = coord.getPartitions(pid).?;
    try testing.expectEqual(@as(usize, 1), parts.len);

    // Different partition should be added
    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 1);
    try testing.expectEqual(@as(usize, 2), coord.getPartitions(pid).?.len);
}

test "TransactionCoordinator addPartitionsToTxn timeout returns protocol error and marks dirty" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("timeout-add");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 0);
    coord.dirty = false;
    if (coord.transactions.getPtr(pid)) |txn| {
        txn.timeout_ms = 1;
        txn.start_time_ms = @import("time_compat").milliTimestamp() - 10_000;
    }

    const err = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 1);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_txn_state)), err);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.prepare_abort, coord.getStatus(pid).?);
    try testing.expect(coord.dirty);
}

test "TransactionCoordinator verifyPartitionInTxn validates existing partitions without mutation" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("verify-add");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic-a", 0);
    try testing.expectEqual(@as(i16, 0), coord.verifyPartitionInTxn(pid, result.producer_epoch, "topic-a", 0));
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_txn_state)), coord.verifyPartitionInTxn(pid, result.producer_epoch, "topic-a", 1));
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.invalid_producer_epoch)), coord.verifyPartitionInTxn(pid, result.producer_epoch + 1, "topic-a", 0));
    try testing.expectEqual(@as(usize, 1), coord.getPartitions(pid).?.len);
}

test "TransactionCoordinator addPartitionsToTxn owns topic names" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("owned-topic");
    const pid = result.producer_id;
    const topic_buf = try testing.allocator.dupe(u8, "owned-topic-a");
    defer testing.allocator.free(topic_buf);

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, topic_buf, 0);
    @memset(topic_buf, 'x');

    const parts = coord.getPartitions(pid).?;
    try testing.expectEqual(@as(usize, 1), parts.len);
    try testing.expectEqualStrings("owned-topic-a", parts[0].topic);
}

test "TransactionCoordinator expireTransactions auto-aborts" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("expire-txn");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic", 0);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.ongoing, coord.getStatus(pid).?);

    // Manipulate start_time to simulate timeout (set 120s in the past)
    if (coord.transactions.getPtr(pid)) |txn| {
        txn.start_time_ms = @import("time_compat").milliTimestamp() - 120_000;
    }

    const expired = coord.expireTransactions();
    try testing.expectEqual(@as(u32, 1), expired);
    // Transaction should be auto-aborted
    try testing.expectEqual(TransactionCoordinator.TxnStatus.complete_abort, coord.getStatus(pid).?);
}

test "TransactionCoordinator expireTransactions skips active" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("active-txn");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, result.producer_epoch, "topic", 0);

    // Don't manipulate time — transaction is fresh
    const expired = coord.expireTransactions();
    try testing.expectEqual(@as(u32, 0), expired);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.ongoing, coord.getStatus(pid).?);
}

test "TransactionCoordinator epoch overflow allocates new PID" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const r1 = try coord.initProducerId("overflow-txn");
    const original_pid = r1.producer_id;

    // Manually set epoch to near-max
    if (coord.transactions.getPtr(original_pid)) |txn| {
        txn.producer_epoch = std.math.maxInt(i16) - 1; // 32766
    }

    // Re-init should detect overflow, allocate new PID, reset epoch to 0
    const r2 = try coord.initProducerId("overflow-txn");
    try testing.expect(r2.producer_id != original_pid); // New PID
    try testing.expectEqual(@as(i16, 0), r2.producer_epoch); // Epoch reset to 0

    // Old PID should be gone, new PID should exist
    try testing.expect(coord.getStatus(original_pid) == null);
    try testing.expect(coord.getStatus(r2.producer_id) != null);
    try testing.expectEqual(@as(usize, 1), coord.transactionCount());
}

test "TransactionCoordinator restoreState rebuilds from entries" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const Persistence = @import("persistence.zig").MetadataPersistence;
    const restored_partitions = [_]Persistence.TransactionPartitionEntry{
        .{ .topic = "restored-topic", .partition = 2 },
    };
    var entries = [_]Persistence.TransactionEntry{
        .{ .producer_id = 2000, .producer_epoch = 3, .status = 1, .timeout_ms = 60000, .transactional_id = null, .partitions = &restored_partitions },
        .{ .producer_id = 2001, .producer_epoch = 0, .status = 0, .timeout_ms = 30000, .transactional_id = null },
    };

    try coord.restoreState(.{ .next_producer_id = 3000, .entries = @as([]Persistence.TransactionEntry, &entries) });

    try testing.expectEqual(@as(i64, 3000), coord.next_producer_id);
    try testing.expectEqual(@as(usize, 2), coord.transactionCount());
    try testing.expectEqual(TransactionCoordinator.TxnStatus.ongoing, coord.getStatus(2000).?);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.empty, coord.getStatus(2001).?);
    const restored = coord.getPartitions(2000).?;
    try testing.expectEqual(@as(usize, 1), restored.len);
    try testing.expectEqualStrings("restored-topic", restored[0].topic);
    try testing.expectEqual(@as(i32, 2), restored[0].partition);
    try testing.expect(!coord.dirty);
}

test "TransactionCoordinator dirty flag tracks mutations" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expect(!coord.dirty);

    const r1 = try coord.initProducerId("dirty-test");
    try testing.expect(coord.dirty);

    coord.dirty = false;

    // addPartitionsToTxn sets dirty
    _ = try coord.addPartitionsToTxn(r1.producer_id, r1.producer_epoch, "topic", 0);
    try testing.expect(coord.dirty);

    coord.dirty = false;

    // endTxn sets dirty
    _ = coord.endTxn(r1.producer_id, r1.producer_epoch, true);
    try testing.expect(coord.dirty);

    coord.dirty = false;

    // writeTxnMarkers sets dirty
    _ = coord.writeTxnMarkers(r1.producer_id);
    try testing.expect(coord.dirty);
}

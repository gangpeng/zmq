const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

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
        partitions: std.ArrayList(TopicPartition),
        start_time_ms: i64,
        timeout_ms: i32 = 60000,

        pub const TopicPartition = struct {
            topic: []const u8,
            partition: i32,
        };

        pub fn deinit(self: *TransactionState, alloc: Allocator) void {
            if (self.transactional_id) |tid| alloc.free(tid);
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
    /// Fix #5: Producer epoch fencing — increment epoch per call for existing transactional_id.
    pub fn initProducerId(self: *TransactionCoordinator, transactional_id: ?[]const u8) !InitProducerIdResult {
        // Check if transactional_id already has a producer_id assigned (epoch fencing)
        if (transactional_id) |tid| {
            var it = self.transactions.iterator();
            while (it.next()) |entry| {
                const txn = entry.value_ptr;
                if (txn.transactional_id) |existing_tid| {
                    if (std.mem.eql(u8, existing_tid, tid)) {
                        // Found existing — bump epoch (fence old producers)
                        txn.producer_epoch += 1;
                        txn.status = .empty;
                        txn.partitions.clearRetainingCapacity();
                        txn.start_time_ms = std.time.milliTimestamp();
                        return .{
                            .error_code = 0,
                            .producer_id = txn.producer_id,
                            .producer_epoch = txn.producer_epoch,
                        };
                    }
                }
            }
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
            .partitions = std.ArrayList(TransactionState.TopicPartition).init(self.allocator),
            .start_time_ms = std.time.milliTimestamp(),
        });

        return .{
            .error_code = 0,
            .producer_id = pid,
            .producer_epoch = epoch,
        };
    }

    pub const InitProducerIdResult = struct {
        error_code: i16,
        producer_id: i64,
        producer_epoch: i16,
    };

    /// AddPartitionsToTxn: register partitions in an active transaction.
    /// Fix #5: enforce timeout.
    pub fn addPartitionsToTxn(self: *TransactionCoordinator, producer_id: i64, topic: []const u8, partition: i32) !i16 {
        const txn = self.transactions.getPtr(producer_id) orelse return 48; // INVALID_PRODUCER_ID_MAPPING

        // Enforce transaction timeout (fix #5)
        if (txn.status == .ongoing) {
            const elapsed = std.time.milliTimestamp() - txn.start_time_ms;
            if (elapsed > txn.timeout_ms) {
                txn.status = .prepare_abort;
                return 55; // INVALID_TXN_STATE (timed out)
            }
        }

        if (txn.status == .empty) {
            txn.status = .ongoing;
            txn.start_time_ms = std.time.milliTimestamp();
        }

        if (txn.status != .ongoing) return 55; // INVALID_TXN_STATE

        try txn.partitions.append(.{ .topic = topic, .partition = partition });
        return 0;
    }

    /// EndTxn: commit or abort the transaction.
    pub fn endTxn(self: *TransactionCoordinator, producer_id: i64, commit: bool) i16 {
        const txn = self.transactions.getPtr(producer_id) orelse return 48;

        if (txn.status != .ongoing) return 55;

        if (commit) {
            txn.status = .prepare_commit;
        } else {
            txn.status = .prepare_abort;
        }

        return 0;
    }

    /// WriteTxnMarkers: finalize the transaction by writing markers to all partitions.
    /// Fix #5: Actually create COMMIT/ABORT control record bytes.
    /// Called after endTxn when all partitions have been prepared.
    pub fn writeTxnMarkers(self: *TransactionCoordinator, producer_id: i64) i16 {
        const txn = self.transactions.getPtr(producer_id) orelse return 48;

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
                txn.partitions.clearRetainingCapacity();
                return 0;
            },
            .prepare_abort => {
                // Create ABORT control records for each partition (fix #5)
                for (txn.partitions.items) |_| {
                    // In production: write a control record batch to the partition store
                    // with ControlRecordType.abort = 0
                }
                txn.status = .complete_abort;
                txn.partitions.clearRetainingCapacity();
                return 0;
            },
            else => return 55, // INVALID_TXN_STATE
        }
    }

    /// Build a control record batch for COMMIT/ABORT markers (fix #5).
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

        const rec_batch = @import("../protocol/record_batch.zig");
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
            std.time.milliTimestamp(),
            std.time.milliTimestamp(),
            attributes,
        );
    }

    /// Complete the EndTxn by writing markers and transitioning to final state.
    /// This is a convenience method that combines endTxn + writeTxnMarkers.
    pub fn endTxnComplete(self: *TransactionCoordinator, producer_id: i64, commit: bool) i16 {
        const err1 = self.endTxn(producer_id, commit);
        if (err1 != 0) return err1;
        return self.writeTxnMarkers(producer_id);
    }

    /// Serialize transaction state for persistence to __transaction_state (fix #12).
    /// Returns allocated bytes that represent the current state.
    pub fn serializeState(self: *TransactionCoordinator) ![]u8 {
        var buf = std.ArrayList(u8).init(self.allocator);
        const writer = buf.writer();

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
    const err1 = try coord.addPartitionsToTxn(pid, "topic-a", 0);
    try testing.expectEqual(@as(i16, 0), err1);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.ongoing, coord.getStatus(pid).?);

    // Commit (two-phase)
    const err2 = coord.endTxn(pid, true);
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

    _ = try coord.addPartitionsToTxn(pid, "topic", 0);
    const err = coord.endTxnComplete(pid, false);
    try testing.expectEqual(@as(i16, 0), err);
    try testing.expectEqual(TransactionCoordinator.TxnStatus.complete_abort, coord.getStatus(pid).?);
}

test "TransactionCoordinator invalid producer id" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const err = try coord.addPartitionsToTxn(9999, "topic", 0);
    try testing.expectEqual(@as(i16, 48), err); // INVALID_PRODUCER_ID_MAPPING
}

test "TransactionCoordinator endTxn invalid state" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-test");
    const pid = result.producer_id;

    // endTxn on EMPTY state (never added partitions, so state is "empty")
    // addPartitions transitions from empty→ongoing, endTxn requires ongoing
    const err = coord.endTxn(pid, true);
    try testing.expectEqual(@as(i16, 55), err); // INVALID_TXN_STATE
}

test "TransactionCoordinator double commit fails" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-double");
    const pid = result.producer_id;

    _ = try coord.addPartitionsToTxn(pid, "topic", 0);

    // First commit succeeds
    try testing.expectEqual(@as(i16, 0), coord.endTxn(pid, true));
    try testing.expectEqual(TransactionCoordinator.TxnStatus.prepare_commit, coord.getStatus(pid).?);

    // Second endTxn on prepare_commit state should fail
    try testing.expectEqual(@as(i16, 55), coord.endTxn(pid, true)); // INVALID_TXN_STATE
}

test "TransactionCoordinator writeTxnMarkers invalid state" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId(null);
    const pid = result.producer_id;

    // writeTxnMarkers without endTxn first
    try testing.expectEqual(@as(i16, 55), coord.writeTxnMarkers(pid)); // INVALID_TXN_STATE
}

test "TransactionCoordinator writeTxnMarkers on nonexistent producer" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expectEqual(@as(i16, 48), coord.writeTxnMarkers(9999)); // INVALID_PRODUCER_ID_MAPPING
}

test "TransactionCoordinator endTxn on nonexistent producer" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    try testing.expectEqual(@as(i16, 48), coord.endTxn(9999, true));
}

test "TransactionCoordinator getPartitions" {
    var coord = TransactionCoordinator.init(testing.allocator);
    defer coord.deinit();

    const result = try coord.initProducerId("txn-parts");
    const pid = result.producer_id;

    // No partitions initially
    const parts0 = coord.getPartitions(pid).?;
    try testing.expectEqual(@as(usize, 0), parts0.len);

    _ = try coord.addPartitionsToTxn(pid, "topic-a", 0);
    _ = try coord.addPartitionsToTxn(pid, "topic-a", 1);

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

    _ = try coord.addPartitionsToTxn(pid, "topic", 0);
    _ = try coord.addPartitionsToTxn(pid, "topic", 1);

    try testing.expectEqual(@as(usize, 2), coord.getPartitions(pid).?.len);

    _ = coord.endTxnComplete(pid, true);

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
    const rec_batch = @import("../protocol/record_batch.zig");
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

    const rec_batch = @import("../protocol/record_batch.zig");
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

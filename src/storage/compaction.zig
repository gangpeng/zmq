const std = @import("std");
const fs = std.fs;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.compaction);

const stream_mod = @import("stream.zig");
const ObjectManager = stream_mod.ObjectManager;
const StreamOffsetRange = stream_mod.StreamOffsetRange;
const ObjectWriter = @import("s3.zig").ObjectWriter;
const ObjectReader = @import("s3.zig").ObjectReader;
const S3Storage = @import("s3_client.zig").S3Storage;
const MockS3 = @import("s3.zig").MockS3;
const MetricRegistry = @import("../core/metric_registry.zig").MetricRegistry;

/// CompactionManager periodically splits multi-stream StreamSetObjects into
/// per-stream StreamObjects, and merges small StreamObjects into larger ones.
///
/// This matches AutoMQ's CompactionManager:
/// - Force split: SSOs with multiple streams → per-stream SOs
/// - Merge: many small SOs for same stream → fewer larger SOs
/// - Cleanup: old SSOs deleted after data is fully split
pub const CompactionManager = struct {
    allocator: Allocator,
    object_manager: *ObjectManager,

    /// S3 backend. We use MockS3 for putObject/getObject/deleteObject.
    /// In production, S3Storage wraps either MockS3 or real S3Client.
    mock_s3: ?*MockS3 = null,
    s3_storage: ?*S3Storage = null,

    /// Interval between compaction runs (default 5 minutes).
    compaction_interval_ms: i64 = 5 * 60 * 1000,

    /// Minimum number of StreamObjects per stream to trigger merge.
    merge_min_object_count: usize = 10,

    /// Maximum size of a merged StreamObject (default 256MB).
    merge_max_size: u64 = 256 * 1024 * 1024,

    /// Timestamp of last compaction run.
    last_compaction_ms: i64 = 0,

    /// Stats
    total_splits: u64 = 0,
    total_merges: u64 = 0,
    total_cleanups: u64 = 0,
    total_destroyed: u64 = 0,
    total_expired_prepared: u64 = 0,

    /// Retention period (ms) for mark_destroyed objects before physical S3 delete.
    /// Default 10 minutes — gives consumers time to finish reading in-flight data.
    ///
    /// NOTE: AutoMQ uses 10 minutes in S3ObjectControlManager. The effective delay
    /// in ZMQ is mark_destroyed_retention_ms ± compaction_interval_ms because
    /// lifecycle checks only run during compaction cycles.
    mark_destroyed_retention_ms: i64 = 10 * 60 * 1000,

    /// TTL (ms) for prepared objects that were never committed.
    /// Default 60 minutes — if a producer crashes between prepareObject() and
    /// commitStreamObject(), the stale allocation is cleaned up here.
    prepared_ttl_ms: i64 = 60 * 60 * 1000,

    /// S3 keys that failed to delete during compaction (orphaned files).
    /// Cleaned up on the next compaction cycle.
    orphaned_keys: std.ArrayList([]u8),

    /// Path to the compaction journal file (tracks in-progress operations).
    journal_path: ?[]const u8 = null,

    /// Optional Prometheus metric registry for compaction observability.
    metrics: ?*MetricRegistry = null,

    pub fn init(
        allocator: Allocator,
        object_manager: *ObjectManager,
    ) CompactionManager {
        return .{
            .allocator = allocator,
            .object_manager = object_manager,
            .orphaned_keys = std.ArrayList([]u8).init(allocator),
        };
    }

    /// Free all tracked orphaned keys and the list itself.
    pub fn deinit(self: *CompactionManager) void {
        for (self.orphaned_keys.items) |key| {
            self.allocator.free(key);
        }
        self.orphaned_keys.deinit();
    }

    /// Set MockS3 backend (for testing).
    pub fn setMockS3(self: *CompactionManager, mock: *MockS3) void {
        self.mock_s3 = mock;
    }

    /// Set S3Storage backend (for production).
    pub fn setS3Storage(self: *CompactionManager, storage: *S3Storage) void {
        self.s3_storage = storage;
    }

    /// Called from Broker.tick(). Checks if enough time has elapsed and runs compaction.
    pub fn maybeCompact(self: *CompactionManager) void {
        const now = std.time.milliTimestamp();
        if (now - self.last_compaction_ms < self.compaction_interval_ms) return;
        self.last_compaction_ms = now;

        self.runCompaction() catch |err| {
            log.warn("Compaction failed: {}", .{err});
            if (self.metrics) |m| {
                m.incrementCounter("compaction_errors_total");
            }
        };
    }

    /// Full compaction cycle: cleanup orphans → force-split → merge →
    /// mark expired → destroy marked → expire prepared.
    ///
    /// NOTE: AutoMQ's CompactionManager uses separate timers for each phase.
    /// ZMQ runs all phases sequentially in one cycle for simplicity. The lifecycle
    /// phases (destroy marked, expire prepared) are added after the core compaction
    /// phases to match AutoMQ's S3ObjectControlManager behavior.
    pub fn runCompaction(self: *CompactionManager) !void {
        const start_ns = std.time.nanoTimestamp();
        log.info("Starting compaction cycle (SSOs={d}, SOs={d})", .{
            self.object_manager.getStreamSetObjectCount(),
            self.object_manager.getStreamObjectCount(),
        });

        // Report orphaned keys gauge at start of cycle
        if (self.metrics) |m| {
            m.setGauge("compaction_orphaned_keys", @floatFromInt(self.orphaned_keys.items.len));
        }

        self.cleanupOrphans();

        self.journalBegin("split", 0);
        const splits = try self.forceSplitAll();
        self.journalComplete();

        self.journalBegin("merge", 0);
        const merges = try self.mergeAll();
        self.journalComplete();

        self.journalBegin("cleanup", 0);
        const cleanups = try self.cleanupExpired();
        self.total_cleanups += cleanups;
        self.journalComplete();

        // Lifecycle Phase 1: Physically delete mark_destroyed objects whose
        // retention period has elapsed. This gives consumers a grace period
        // to finish reading data before it disappears from S3.
        self.journalBegin("destroy", 0);
        const destroyed = self.cleanupDestroyed();
        self.total_destroyed += destroyed;
        self.journalComplete();

        // Lifecycle Phase 2: Expire prepared objects that were never committed
        // (e.g., producer crashed between prepareObject and commitStreamObject).
        const expired_prepared = self.object_manager.expirePreparedObjects(self.prepared_ttl_ms);
        self.total_expired_prepared += expired_prepared;
        if (expired_prepared > 0) {
            log.info("Expired {d} prepared objects (TTL={d}ms)", .{ expired_prepared, self.prepared_ttl_ms });
        }

        self.total_splits += splits;
        self.total_merges += merges;

        // Record Prometheus metrics
        if (self.metrics) |m| {
            m.incrementCounter("compaction_cycles_total");
            m.addCounter("compaction_splits_total", splits);
            m.addCounter("compaction_merges_total", merges);
            m.addCounter("compaction_cleanups_total", cleanups);
            m.addCounter("compaction_destroyed_total", destroyed);
            m.addCounter("compaction_expired_prepared_total", expired_prepared);
            const elapsed_ns = std.time.nanoTimestamp() - start_ns;
            const elapsed_secs: f64 = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
            m.observeHistogram("compaction_cycle_duration_seconds", elapsed_secs);
            m.setGauge("compaction_orphaned_keys", @floatFromInt(self.orphaned_keys.items.len));
        }

        log.info("Compaction complete: {d} splits, {d} merges, {d} cleanups, {d} destroyed, {d} expired prepared", .{
            splits, merges, cleanups, destroyed, expired_prepared,
        });
    }

    /// Clean up S3 objects that were orphaned by previous failed deletions.
    fn cleanupOrphans(self: *CompactionManager) void {
        if (self.orphaned_keys.items.len > 0) {
            log.info("Cleaning up {d} orphaned S3 objects from previous cycle", .{self.orphaned_keys.items.len});
        }
        var i: usize = 0;
        while (i < self.orphaned_keys.items.len) {
            const key = self.orphaned_keys.items[i];
            if (self.deleteS3Object(key)) {
                self.allocator.free(key);
                _ = self.orphaned_keys.swapRemove(i);
                self.total_cleanups += 1;
            } else {
                i += 1; // Retry next cycle
            }
        }
    }

    // ---- Phase 1: Force Split ----

    /// Split all multi-stream SSOs into per-stream StreamObjects.
    fn forceSplitAll(self: *CompactionManager) !u64 {
        var split_count: u64 = 0;

        // Collect SSO IDs to split (can't modify map while iterating)
        var to_split = std.ArrayList(u64).init(self.allocator);
        defer to_split.deinit();

        var it = self.object_manager.stream_set_objects.iterator();
        while (it.next()) |entry| {
            // Only split committed SSOs — skip prepared (not yet uploaded) and
            // mark_destroyed (pending deletion, don't create new SOs from them)
            if (entry.value_ptr.state != .committed) continue;
            if (!entry.value_ptr.isSingleStream()) {
                try to_split.append(entry.value_ptr.object_id);
            }
        }

        for (to_split.items) |sso_id| {
            self.splitStreamSetObject(sso_id) catch |err| {
                log.warn("Failed to split SSO {d}: {}", .{ sso_id, err });
                continue;
            };
            split_count += 1;
        }

        return split_count;
    }

    /// Split a single SSO: read from S3, extract per-stream data,
    /// write individual StreamObjects, remove old SSO.
    fn splitStreamSetObject(self: *CompactionManager, sso_id: u64) !void {
        const sso = self.object_manager.stream_set_objects.get(sso_id) orelse return;

        // 1. Read the SSO data from S3
        const obj_data = try self.getS3Object(sso.s3_key);
        defer self.allocator.free(obj_data);

        // 2. Parse with ObjectReader
        var reader = try ObjectReader.parse(self.allocator, obj_data);
        defer reader.deinit();

        // 3. Get distinct stream IDs
        // We need to borrow the SSO again after potential modifications
        const sso_ref = self.object_manager.stream_set_objects.get(sso_id) orelse return;
        const stream_ids = try sso_ref.distinctStreamIds(self.allocator);
        defer self.allocator.free(stream_ids);

        // 4. For each stream, collect its blocks and write a StreamObject
        for (stream_ids) |sid| {
            var writer = ObjectWriter.init(self.allocator);
            defer writer.deinit();

            var so_start_offset: u64 = std.math.maxInt(u64);
            var so_end_offset: u64 = 0;
            var block_count: usize = 0;

            for (reader.index_entries, 0..) |entry, i| {
                if (entry.stream_id != sid) continue;

                const block_data = reader.readBlock(i) orelse continue;
                try writer.addDataBlock(
                    entry.stream_id,
                    entry.start_offset,
                    entry.end_offset_delta,
                    entry.record_count,
                    block_data,
                );

                so_start_offset = @min(so_start_offset, entry.start_offset);
                const entry_end = entry.start_offset + entry.end_offset_delta;
                so_end_offset = @max(so_end_offset, entry_end);
                block_count += 1;
            }

            if (block_count == 0) continue;

            // Idempotency check: skip if an SO already covers this exact range
            // (can happen if a previous split was interrupted and replayed)
            if (self.object_manager.hasStreamObjectCovering(sid, so_start_offset, so_end_offset)) {
                log.info("Skipping duplicate SO for stream {d} [{d}..{d})", .{ sid, so_start_offset, so_end_offset });
                continue;
            }

            const so_data = try writer.build();
            defer self.allocator.free(so_data);

            // 5. Upload new StreamObject to S3
            const new_obj_id = self.object_manager.allocateObjectId();
            const so_key = try std.fmt.allocPrint(
                self.allocator,
                "data/so/{d}/{d}-{d}",
                .{ sid, so_start_offset, new_obj_id },
            );
            defer self.allocator.free(so_key);

            try self.putS3Object(so_key, so_data);

            // 6. Register with ObjectManager
            try self.object_manager.commitStreamObject(
                new_obj_id,
                sid,
                so_start_offset,
                so_end_offset,
                so_key,
                so_data.len,
            );

            log.info("Split SSO {d}: created SO {d} for stream {d} [{d}..{d})", .{
                sso_id, new_obj_id, sid, so_start_offset, so_end_offset,
            });
        }

        // 7. Remove SSO from ObjectManager first (so fetch queries no longer see it),
        // then delete from S3. This is the write-before-delete pattern:
        //   - All new SOs are registered (step 6) before SSO is removed
        //   - If we crash after removing from ObjectManager but before S3 delete,
        //     the SSO file becomes an orphan in S3 (harmless, wasted space)
        //   - If we crash before removing from ObjectManager, the SSO and SOs both
        //     exist in the index, but getObjects() prefers SOs over SSOs at the same
        //     offset, so consumers won't see duplicates
        const sso_key_copy = if (self.object_manager.stream_set_objects.get(sso_id)) |sso_final|
            self.allocator.dupe(u8, sso_final.s3_key) catch null
        else
            null;
        self.object_manager.removeStreamSetObject(sso_id);
        if (sso_key_copy) |key| {
            if (!self.deleteS3Object(key)) {
                log.warn("Orphaned SSO in S3: '{s}' (will be cleaned up later)", .{key});
                self.orphaned_keys.append(key) catch self.allocator.free(key);
            } else {
                self.allocator.free(key);
            }
        }
    }

    // ---- Phase 2: Merge ----

    /// For each stream with many small StreamObjects, merge them.
    fn mergeAll(self: *CompactionManager) !u64 {
        var merge_count: u64 = 0;

        // Collect stream IDs to consider (can't modify while iterating)
        var stream_ids = std.ArrayList(u64).init(self.allocator);
        defer stream_ids.deinit();

        var it = self.object_manager.stream_object_index.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.items.len >= self.merge_min_object_count) {
                try stream_ids.append(entry.key_ptr.*);
            }
        }

        for (stream_ids.items) |sid| {
            // Re-check after potential modifications
            const so_ids_list = self.object_manager.stream_object_index.get(sid) orelse continue;
            if (so_ids_list.items.len < 2) continue;

            // Copy object IDs to avoid issues with iterator invalidation
            const so_ids = try self.allocator.dupe(u64, so_ids_list.items);
            defer self.allocator.free(so_ids);

            // Group contiguous objects for merging
            var group = std.ArrayList(u64).init(self.allocator);
            defer group.deinit();
            var group_size: u64 = 0;

            for (so_ids) |obj_id| {
                const so = self.object_manager.stream_objects.get(obj_id) orelse continue;
                // Only merge committed objects — skip prepared and mark_destroyed
                if (so.state != .committed) continue;

                if (group_size + so.object_size > self.merge_max_size and group.items.len > 1) {
                    // Flush current merge group
                    self.mergeStreamObjects(sid, group.items) catch |err| {
                        log.warn("Merge failed for stream {d}: {}", .{ sid, err });
                        group.clearRetainingCapacity();
                        group_size = 0;
                        continue;
                    };
                    merge_count += 1;
                    group.clearRetainingCapacity();
                    group_size = 0;
                }

                try group.append(obj_id);
                group_size += so.object_size;
            }

            // Merge remaining group if ≥ 2 objects
            if (group.items.len >= 2) {
                self.mergeStreamObjects(sid, group.items) catch |err| {
                    log.warn("Merge failed for stream {d}: {}", .{ sid, err });
                    continue;
                };
                merge_count += 1;
            }
        }

        return merge_count;
    }

    /// Merge a list of StreamObject IDs for a single stream into one.
    fn mergeStreamObjects(self: *CompactionManager, stream_id: u64, object_ids: []const u64) !void {
        if (object_ids.len < 2) return;

        var writer = ObjectWriter.init(self.allocator);
        defer writer.deinit();

        var merged_start: u64 = std.math.maxInt(u64);
        var merged_end: u64 = 0;

        // Read each SO, extract blocks, append to new writer.
        // If ANY SO fails to read, abort the entire merge to prevent data loss.
        var read_failures: usize = 0;
        for (object_ids) |obj_id| {
            const so = self.object_manager.stream_objects.get(obj_id) orelse {
                read_failures += 1;
                continue;
            };

            const obj_data = self.getS3Object(so.s3_key) catch |err| {
                log.warn("Failed to read SO {d} for merge: {} — aborting merge", .{ obj_id, err });
                read_failures += 1;
                continue;
            };
            defer self.allocator.free(obj_data);

            var reader = ObjectReader.parse(self.allocator, obj_data) catch |err| {
                log.warn("Failed to parse SO {d}: {} — aborting merge", .{ obj_id, err });
                read_failures += 1;
                continue;
            };
            defer reader.deinit();

            for (reader.index_entries, 0..) |entry, i| {
                const block_data = reader.readBlock(i) orelse continue;
                try writer.addDataBlock(
                    entry.stream_id,
                    entry.start_offset,
                    entry.end_offset_delta,
                    entry.record_count,
                    block_data,
                );
                merged_start = @min(merged_start, entry.start_offset);
                const entry_end = entry.start_offset + entry.end_offset_delta;
                merged_end = @max(merged_end, entry_end);
            }
        }

        // Abort if any SO failed to read — we can't safely delete old SOs
        // if we didn't read all their data into the merged object.
        if (read_failures > 0) {
            log.warn("Merge aborted for stream {d}: {d}/{d} SOs failed to read", .{
                stream_id, read_failures, object_ids.len,
            });
            return error.MergeAbortedPartialRead;
        }
        if (merged_start == std.math.maxInt(u64)) return; // No data to merge

        // Write merged object
        const merged_data = try writer.build();
        defer self.allocator.free(merged_data);

        const new_obj_id = self.object_manager.allocateObjectId();
        const merged_key = try std.fmt.allocPrint(
            self.allocator,
            "data/so/{d}/{d}-{d}",
            .{ stream_id, merged_start, new_obj_id },
        );
        defer self.allocator.free(merged_key);

        try self.putS3Object(merged_key, merged_data);
        try self.object_manager.commitStreamObject(
            new_obj_id,
            stream_id,
            merged_start,
            merged_end,
            merged_key,
            merged_data.len,
        );

        // Remove old SOs: first collect S3 keys, then remove from ObjectManager,
        // then delete from S3. This ensures fetch queries see the merged object
        // and not the old SOs, even if S3 deletion fails (orphaned files).
        var old_keys = std.ArrayList([]u8).init(self.allocator);
        defer {
            for (old_keys.items) |k| self.allocator.free(k);
            old_keys.deinit();
        }
        for (object_ids) |obj_id| {
            if (self.object_manager.stream_objects.get(obj_id)) |so| {
                const key_copy = self.allocator.dupe(u8, so.s3_key) catch continue;
                old_keys.append(key_copy) catch {
                    self.allocator.free(key_copy);
                };
            }
            self.object_manager.removeStreamObject(obj_id);
        }
        // Now delete from S3 (ObjectManager no longer references these)
        for (old_keys.items) |key| {
            if (!self.deleteS3Object(key)) {
                log.warn("Orphaned SO in S3: '{s}'", .{key});
                const orphan_copy = self.allocator.dupe(u8, key) catch continue;
                self.orphaned_keys.append(orphan_copy) catch self.allocator.free(orphan_copy);
            }
        }

        log.info("Merged {d} SOs into SO {d} for stream {d} [{d}..{d})", .{
            object_ids.len, new_obj_id, stream_id, merged_start, merged_end,
        });
    }

    // ---- Phase 3: Cleanup Expired ----

    /// Phase 3: Mark expired StreamObjects for destruction. Objects whose data
    /// is fully behind the stream's start_offset (retention-trimmed) transition
    /// to mark_destroyed state instead of being immediately deleted.
    ///
    /// NOTE: AutoMQ's S3ObjectControlManager delays physical S3 deletion by
    /// 10 minutes after marking an object destroyed. This gives consumers that
    /// are mid-fetch time to complete their reads. ZMQ achieves the same by
    /// setting mark_destroyed state here and physically deleting in
    /// cleanupDestroyed() after mark_destroyed_retention_ms has elapsed.
    fn cleanupExpired(self: *CompactionManager) !u64 {
        var cleanup_count: u64 = 0;

        var it = self.object_manager.stream_object_index.iterator();
        while (it.next()) |entry| {
            const stream_id = entry.key_ptr.*;
            const stream = self.object_manager.streams.get(stream_id) orelse continue;

            // Find committed SOs that are fully behind the stream's start_offset
            for (entry.value_ptr.items) |obj_id| {
                const so = self.object_manager.stream_objects.get(obj_id) orelse continue;
                if (so.state != .committed) continue;
                if (so.end_offset <= stream.start_offset) {
                    self.object_manager.markDestroyed(obj_id);
                    cleanup_count += 1;
                }
            }
        }

        if (cleanup_count > 0) {
            log.info("Marked {d} expired StreamObjects for destruction", .{cleanup_count});
        }
        return cleanup_count;
    }

    // ---- Phase 4: Destroy Marked Objects ----

    /// Phase 4: Physically delete mark_destroyed objects whose retention period
    /// has elapsed. Uses write-before-delete: collectDestroyedObjects removes
    /// objects from metadata first, then we delete from S3. Failed S3 deletes
    /// are tracked in orphaned_keys for retry on the next cycle.
    fn cleanupDestroyed(self: *CompactionManager) u64 {
        const keys = self.object_manager.collectDestroyedObjects(
            self.mark_destroyed_retention_ms,
            self.allocator,
        ) catch |err| {
            log.warn("Failed to collect destroyed objects: {}", .{err});
            return 0;
        };
        defer self.allocator.free(keys);

        var destroyed_count: u64 = 0;
        for (keys) |key| {
            if (!self.deleteS3Object(key)) {
                log.warn("Orphaned destroyed object in S3: '{s}'", .{key});
                self.orphaned_keys.append(key) catch self.allocator.free(key);
            } else {
                self.allocator.free(key);
            }
            destroyed_count += 1;
        }

        if (destroyed_count > 0) {
            log.info("Physically deleted {d} destroyed objects from S3", .{destroyed_count});
        }
        return destroyed_count;
    }

    // ---- Compaction Journal ----

    /// Write a journal entry before starting a compaction operation.
    fn journalBegin(self: *CompactionManager, op: []const u8, object_id: u64) void {
        const dir = self.journal_path orelse return;
        const path = std.fmt.allocPrint(self.allocator, "{s}/compaction.journal", .{dir}) catch |err| {
            log.warn("Failed to allocate compaction journal path: {}", .{err});
            return;
        };
        defer self.allocator.free(path);

        const file = fs.createFileAbsolute(path, .{ .truncate = true }) catch |err| {
            log.warn("Failed to create compaction journal: {}", .{err});
            return;
        };
        defer file.close();

        var buf: [64]u8 = undefined;
        var pos: usize = 0;
        // Write: op_len(u8) + op + object_id(u64)
        buf[pos] = @intCast(op.len);
        pos += 1;
        @memcpy(buf[pos .. pos + op.len], op);
        pos += op.len;
        std.mem.writeInt(u64, buf[pos..][0..8], object_id, .big);
        pos += 8;
        file.writeAll(buf[0..pos]) catch |err| {
            log.warn("Failed to write compaction journal: {}", .{err});
        };
    }

    /// Clear the journal after successful completion.
    fn journalComplete(self: *CompactionManager) void {
        const dir = self.journal_path orelse return;
        const path = std.fmt.allocPrint(self.allocator, "{s}/compaction.journal", .{dir}) catch |err| {
            log.warn("Failed to allocate journal path for cleanup: {}", .{err});
            return;
        };
        defer self.allocator.free(path);
        fs.deleteFileAbsolute(path) catch |err| {
            log.warn("Failed to delete compaction journal: {}", .{err});
        };
    }

    // ---- S3 helpers (abstract over MockS3 vs S3Storage) ----

    fn getS3Object(self: *CompactionManager, key: []const u8) ![]u8 {
        if (self.mock_s3) |s3| {
            const data = s3.getObject(key) orelse return error.S3ObjectNotFound;
            return try self.allocator.dupe(u8, data);
        }
        if (self.s3_storage) |s3| {
            const data = (try s3.getObject(key)) orelse return error.S3ObjectNotFound;
            return data;
        }
        return error.NoS3Backend;
    }

    fn putS3Object(self: *CompactionManager, key: []const u8, data: []const u8) !void {
        if (self.mock_s3) |s3| {
            try s3.putObject(key, data);
            return;
        }
        if (self.s3_storage) |s3| {
            try s3.putObject(key, data);
            return;
        }
        return error.NoS3Backend;
    }

    /// Delete an S3 object. Returns true if deleted, false if failed.
    /// Failures are logged but do not stop compaction — orphaned S3 objects
    /// are cleaned up on the next compaction cycle.
    fn deleteS3Object(self: *CompactionManager, key: []const u8) bool {
        if (self.mock_s3) |s3| {
            _ = s3.deleteObject(key);
            return true;
        }
        if (self.s3_storage) |s3| {
            s3.deleteObject(key) catch |err| {
                log.warn("Failed to delete S3 object '{s}': {}", .{ key, err });
                return false;
            };
            return true;
        }
        return false;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "CompactionManager forceSplit splits multi-stream SSO" {
    // Setup: ObjectManager + MockS3
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create a multi-stream S3 object using ObjectWriter
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 10, 10, "stream1-block-data-a");
    try writer.addDataBlock(2, 0, 5, 5, "stream2-block-data");
    try writer.addDataBlock(1, 10, 5, 5, "stream1-block-data-b");
    try writer.addDataBlock(3, 0, 3, 3, "stream3-block-data");

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Upload to mock S3
    try mock_s3.putObject("sso/0/1", obj_data);

    // Register as SSO in ObjectManager
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 15 },
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 5 },
        .{ .stream_id = 3, .start_offset = 0, .end_offset = 3 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/0/1", obj_data.len);

    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());

    // Run compaction
    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);

    try cm.runCompaction();

    // Verify: SSO should be gone, 3 SOs should exist
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 3), om.getStreamObjectCount());

    // Verify we can query each stream
    const r1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 1), r1.len);
    try testing.expectEqual(stream_mod.S3ObjectType.stream, r1[0].object_type);
    try testing.expectEqual(@as(u64, 0), r1[0].start_offset);
    try testing.expectEqual(@as(u64, 15), r1[0].end_offset);

    const r2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 1), r2.len);

    const r3 = try om.getObjects(3, 0, 100, 10);
    defer testing.allocator.free(r3);
    try testing.expectEqual(@as(usize, 1), r3.len);

    // Verify the split SOs are readable from S3
    var so_it = om.stream_objects.iterator();
    const so1 = so_it.next().?.value_ptr;
    const so1_data = mock_s3.getObject(so1.s3_key);
    try testing.expect(so1_data != null);

    try testing.expectEqual(@as(u64, 1), cm.total_splits);
}

test "CompactionManager does not split single-stream SSO" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 10, 10, "data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    try mock_s3.putObject("sso/0/1", obj_data);
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 10 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/0/1", obj_data.len);

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);

    try cm.runCompaction();

    // Single-stream SSO should NOT be split
    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());
    try testing.expectEqual(@as(u64, 0), cm.total_splits);
}

test "CompactionManager merge combines small StreamObjects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create 12 small StreamObjects for stream 1
    var i: u64 = 0;
    while (i < 12) : (i += 1) {
        var writer = ObjectWriter.init(testing.allocator);
        defer writer.deinit();

        const block_data = "record-data-block";
        try writer.addDataBlock(1, i * 10, 10, 10, block_data);

        const obj_data = try writer.build();
        defer testing.allocator.free(obj_data);

        const key = try std.fmt.allocPrint(testing.allocator, "so/1/{d}", .{i});
        defer testing.allocator.free(key);

        try mock_s3.putObject(key, obj_data);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, obj_data.len);
    }

    try testing.expectEqual(@as(usize, 12), om.getStreamObjectCount());

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;

    try cm.runCompaction();

    // Should have merged into 1 StreamObject
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());
    try testing.expect(cm.total_merges > 0);

    // Verify merged SO covers full range
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 0), results[0].start_offset);
    try testing.expectEqual(@as(u64, 120), results[0].end_offset);
}

test "CompactionManager maybeCompact respects interval" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.compaction_interval_ms = 60_000; // 60 seconds

    // First call should set the timestamp but not run (no S3 backend)
    cm.maybeCompact();
    const first_ts = cm.last_compaction_ms;
    try testing.expect(first_ts > 0);

    // Second call immediately should NOT run (< 60s)
    cm.maybeCompact();
    // last_compaction_ms should not have changed significantly
    try testing.expect(cm.last_compaction_ms == first_ts);
}

test "CompactionManager merge skips streams with too few objects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create only 3 StreamObjects (below merge_min_object_count=10)
    var i: u64 = 0;
    while (i < 3) : (i += 1) {
        var writer = ObjectWriter.init(testing.allocator);
        defer writer.deinit();
        try writer.addDataBlock(1, i * 10, 10, 10, "data");
        const obj_data = try writer.build();
        defer testing.allocator.free(obj_data);

        const key = try std.fmt.allocPrint(testing.allocator, "so/1/{d}", .{i});
        defer testing.allocator.free(key);

        try mock_s3.putObject(key, obj_data);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, obj_data.len);
    }

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;

    try cm.runCompaction();

    // Should NOT have merged (only 3 objects, threshold is 10)
    try testing.expectEqual(@as(usize, 3), om.getStreamObjectCount());
    try testing.expectEqual(@as(u64, 0), cm.total_merges);
}

test "CompactionManager forceSplit preserves data integrity" {
    // Verify that after splitting, the data is byte-for-byte identical
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    const s1_data = "STREAM-1-RECORD-DATA-BLOCK";
    const s2_data = "STREAM-2-DIFFERENT-DATA";

    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();
    try writer.addDataBlock(1, 0, 5, 5, s1_data);
    try writer.addDataBlock(2, 0, 3, 3, s2_data);

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    try mock_s3.putObject("sso/0/1", obj_data);

    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 5 },
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 3 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/0/1", obj_data.len);

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    try cm.runCompaction();

    // Verify data integrity: read each StreamObject and check block data
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());

    // Find and verify stream 1's data
    const r1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 1), r1.len);
    const so1_data = mock_s3.getObject(r1[0].s3_key).?;
    var reader1 = try ObjectReader.parse(testing.allocator, so1_data);
    defer reader1.deinit();
    try testing.expectEqualStrings(s1_data, reader1.readBlock(0).?);

    // Find and verify stream 2's data
    const r2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 1), r2.len);
    const so2_data = mock_s3.getObject(r2[0].s3_key).?;
    var reader2 = try ObjectReader.parse(testing.allocator, so2_data);
    defer reader2.deinit();
    try testing.expectEqualStrings(s2_data, reader2.readBlock(0).?);
}

test "CompactionManager merge preserves data integrity" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create 12 SOs with known data
    const expected_blocks: [12][]const u8 = .{
        "block-00", "block-01", "block-02", "block-03",
        "block-04", "block-05", "block-06", "block-07",
        "block-08", "block-09", "block-10", "block-11",
    };

    var i: u64 = 0;
    while (i < 12) : (i += 1) {
        var w = ObjectWriter.init(testing.allocator);
        defer w.deinit();
        try w.addDataBlock(1, i * 10, 10, 10, expected_blocks[i]);
        const d = try w.build();
        defer testing.allocator.free(d);

        const key = try std.fmt.allocPrint(testing.allocator, "so/1/{d}", .{i});
        defer testing.allocator.free(key);
        try mock_s3.putObject(key, d);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, d.len);
    }

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;
    try cm.runCompaction();

    // Verify merged object contains all 12 blocks in order
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);

    const merged_data = mock_s3.getObject(results[0].s3_key).?;
    var reader = try ObjectReader.parse(testing.allocator, merged_data);
    defer reader.deinit();

    try testing.expectEqual(@as(usize, 12), reader.index_entries.len);
    i = 0;
    while (i < 12) : (i += 1) {
        const block = reader.readBlock(i).?;
        try testing.expectEqualStrings(expected_blocks[i], block);
    }
}

test "CompactionManager split uses write-before-delete pattern" {
    // Verify that after a split, the SSO is removed from ObjectManager
    // (no duplicate data in fetch queries)
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();
    try writer.addDataBlock(1, 0, 10, 10, "stream1-data");
    try writer.addDataBlock(2, 0, 5, 5, "stream2-data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    try mock_s3.putObject("sso/test/1", obj_data);
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 10 },
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 5 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/test/1", obj_data.len);

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    try cm.runCompaction();

    // After split: SSO gone, 2 SOs exist
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());

    // Fetch for stream 1 should return exactly ONE object (not SSO + SO)
    const r1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 1), r1.len);

    // Fetch for stream 2 should also return exactly ONE object
    const r2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 1), r2.len);
}

test "CompactionManager merge aborts if SO read fails" {
    // If one SO can't be read from S3, the merge should abort entirely
    // to prevent data loss (we can't merge what we can't read).
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create 12 SOs, but delete one from S3 to simulate read failure
    var i: u64 = 0;
    while (i < 12) : (i += 1) {
        var w = ObjectWriter.init(testing.allocator);
        defer w.deinit();
        try w.addDataBlock(1, i * 10, 10, 10, "data");
        const d = try w.build();
        defer testing.allocator.free(d);

        const key = try std.fmt.allocPrint(testing.allocator, "so/fail/{d}", .{i});
        defer testing.allocator.free(key);
        try mock_s3.putObject(key, d);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, d.len);
    }

    // Delete SO #5 from S3 (but keep in ObjectManager) to simulate S3 failure
    _ = mock_s3.deleteObject("so/fail/5");

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;

    // runCompaction should handle the merge failure gracefully
    try cm.runCompaction();

    // All 12 SOs should still exist (merge was aborted, no deletions)
    try testing.expectEqual(@as(usize, 12), om.getStreamObjectCount());
    try testing.expectEqual(@as(u64, 0), cm.total_merges);
}

test "CompactionManager merge removes old SOs from ObjectManager before S3" {
    // Verify write-before-delete: merged SO registered and old SOs removed
    // from ObjectManager before S3 deletion
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    var i: u64 = 0;
    while (i < 12) : (i += 1) {
        var w = ObjectWriter.init(testing.allocator);
        defer w.deinit();
        try w.addDataBlock(1, i * 10, 10, 10, "merge-data");
        const d = try w.build();
        defer testing.allocator.free(d);

        const key = try std.fmt.allocPrint(testing.allocator, "so/merge/{d}", .{i});
        defer testing.allocator.free(key);
        try mock_s3.putObject(key, d);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, d.len);
    }

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;
    try cm.runCompaction();

    // After merge: exactly 1 SO in ObjectManager
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());

    // The merged SO should cover the full range
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 0), results[0].start_offset);
    try testing.expectEqual(@as(u64, 120), results[0].end_offset);
}

test "CompactionManager deleteS3Object failure does not crash" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // No S3 backend configured — deletes should return false (not crash)
    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    const result = cm.deleteS3Object("nonexistent-key");
    try testing.expect(!result);
}

test "CompactionManager split then merge preserves data end-to-end" {
    // Full pipeline: create multi-stream SSO, split, then merge the resulting SOs
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create 12 multi-stream SSOs (each has streams 1 and 2)
    var j: u64 = 0;
    while (j < 12) : (j += 1) {
        var w = ObjectWriter.init(testing.allocator);
        defer w.deinit();
        try w.addDataBlock(1, j * 10, 10, 10, "s1-data");
        try w.addDataBlock(2, j * 5, 5, 5, "s2-data");
        const d = try w.build();
        defer testing.allocator.free(d);

        const key = try std.fmt.allocPrint(testing.allocator, "sso/e2e/{d}", .{j});
        defer testing.allocator.free(key);
        try mock_s3.putObject(key, d);

        const ranges = [_]StreamOffsetRange{
            .{ .stream_id = 1, .start_offset = j * 10, .end_offset = (j + 1) * 10 },
            .{ .stream_id = 2, .start_offset = j * 5, .end_offset = (j + 1) * 5 },
        };
        try om.commitStreamSetObject(j + 1, 0, j + 1, &ranges, key, d.len);
    }

    try testing.expectEqual(@as(usize, 12), om.getStreamSetObjectCount());

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    // Set merge threshold high so the first compaction only splits (no merge)
    cm.merge_min_object_count = 100;

    // Phase 1: Split all multi-stream SSOs
    try cm.runCompaction();

    // After split: 0 SSOs, 24 SOs (12 for stream 1, 12 for stream 2)
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 24), om.getStreamObjectCount());

    // Phase 2: Run again to merge (now each stream has 12 SOs ≥ threshold of 10)
    cm.merge_min_object_count = 10;
    cm.last_compaction_ms = 0; // Reset timer
    try cm.runCompaction();

    // After merge: each stream should have 1 merged SO = 2 total
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());

    // Verify both streams queryable
    const r1 = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(r1);
    try testing.expect(r1.len >= 1);

    const r2 = try om.getObjects(2, 0, 200, 10);
    defer testing.allocator.free(r2);
    try testing.expect(r2.len >= 1);
}

test "CompactionManager cleans up orphaned S3 keys" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);

    // Simulate orphaned keys by adding them directly
    const key1 = try testing.allocator.dupe(u8, "orphan/key1");
    const key2 = try testing.allocator.dupe(u8, "orphan/key2");
    try cm.orphaned_keys.append(key1);
    try cm.orphaned_keys.append(key2);

    // Put objects in S3 so deleteObject succeeds (MockS3 always returns true for delete,
    // but let's add them for realism)
    try mock_s3.putObject("orphan/key1", "data1");
    try mock_s3.putObject("orphan/key2", "data2");

    try testing.expectEqual(@as(usize, 2), cm.orphaned_keys.items.len);

    // Run compaction — cleanupOrphans should clear them
    try cm.runCompaction();

    // Orphaned keys should be cleaned up
    try testing.expectEqual(@as(usize, 0), cm.orphaned_keys.items.len);
    try testing.expectEqual(@as(u64, 2), cm.total_cleanups);
}

test "CompactionManager cleanupExpired marks retention-trimmed SOs for destruction" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create a stream and advance its start_offset (simulating retention trim)
    const stream = try om.createStreamWithId(1, 0);
    stream.advanceEndOffset(200);
    stream.trim(100); // Data before offset 100 is expired

    // Create SOs: one fully behind start_offset, one straddling, one ahead
    var w1 = ObjectWriter.init(testing.allocator);
    defer w1.deinit();
    try w1.addDataBlock(1, 0, 50, 50, "expired-data");
    const d1 = try w1.build();
    defer testing.allocator.free(d1);
    try mock_s3.putObject("so/1/0-expired", d1);
    try om.commitStreamObject(10, 1, 0, 50, "so/1/0-expired", d1.len);

    var w2 = ObjectWriter.init(testing.allocator);
    defer w2.deinit();
    try w2.addDataBlock(1, 50, 50, 50, "also-expired");
    const d2 = try w2.build();
    defer testing.allocator.free(d2);
    try mock_s3.putObject("so/1/50-expired", d2);
    try om.commitStreamObject(11, 1, 50, 100, "so/1/50-expired", d2.len);

    var w3 = ObjectWriter.init(testing.allocator);
    defer w3.deinit();
    try w3.addDataBlock(1, 100, 50, 50, "live-data");
    const d3 = try w3.build();
    defer testing.allocator.free(d3);
    try mock_s3.putObject("so/1/100-live", d3);
    try om.commitStreamObject(12, 1, 100, 150, "so/1/100-live", d3.len);

    try testing.expectEqual(@as(usize, 3), om.getStreamObjectCount());

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    // Use zero retention so destroy happens in the same cycle
    cm.mark_destroyed_retention_ms = 0;

    try cm.runCompaction();

    // The two expired SOs (end_offset <= 100) should be marked destroyed
    // and then physically deleted (retention=0)
    // The live SO (start_offset=100, end_offset=150) should remain
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());

    // Verify the remaining SO is the live one
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 100), results[0].start_offset);
    try testing.expectEqual(@as(u64, 150), results[0].end_offset);
}

test "CompactionManager idempotent split skips existing SOs" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create a multi-stream SSO
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();
    try writer.addDataBlock(1, 0, 10, 10, "stream1-data");
    try writer.addDataBlock(2, 0, 5, 5, "stream2-data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    try mock_s3.putObject("sso/0/1", obj_data);
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 10 },
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 5 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/0/1", obj_data.len);

    // Pre-register an SO that covers stream 1's exact range [0, 10)
    // to simulate a previous interrupted split
    var pre_writer = ObjectWriter.init(testing.allocator);
    defer pre_writer.deinit();
    try pre_writer.addDataBlock(1, 0, 10, 10, "stream1-data");
    const pre_data = try pre_writer.build();
    defer testing.allocator.free(pre_data);
    try mock_s3.putObject("so/1/pre-existing", pre_data);
    try om.commitStreamObject(50, 1, 0, 10, "so/1/pre-existing", pre_data.len);

    // 1 SSO + 1 pre-existing SO
    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);

    try cm.runCompaction();

    // SSO should be gone
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());
    // Should have 2 SOs: the pre-existing one for stream 1, and a new one for stream 2
    // Stream 1's split should have been skipped (idempotent)
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());

    // Verify stream 1 still has exactly 1 SO (the pre-existing one, not a duplicate)
    const r1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 1), r1.len);
    try testing.expectEqual(@as(u64, 0), r1[0].start_offset);
    try testing.expectEqual(@as(u64, 10), r1[0].end_offset);

    // Verify stream 2 has its SO
    const r2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 1), r2.len);
}

// ---------------------------------------------------------------
// S3 Object Lifecycle — Compaction integration tests
// ---------------------------------------------------------------

test "CompactionManager cleanupExpired uses markDestroyed not immediate delete" {
    // After cleanupExpired, objects should be in mark_destroyed state,
    // NOT removed from ObjectManager. They remain visible to ObjectManager
    // (though hidden from getObjects) until cleanupDestroyed runs.
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    const stream = try om.createStreamWithId(1, 0);
    stream.advanceEndOffset(200);
    stream.trim(100);

    var w1 = ObjectWriter.init(testing.allocator);
    defer w1.deinit();
    try w1.addDataBlock(1, 0, 50, 50, "expired-data");
    const d1 = try w1.build();
    defer testing.allocator.free(d1);
    try mock_s3.putObject("so/1/0-expired", d1);
    try om.commitStreamObject(10, 1, 0, 50, "so/1/0-expired", d1.len);

    var w2 = ObjectWriter.init(testing.allocator);
    defer w2.deinit();
    try w2.addDataBlock(1, 100, 50, 50, "live-data");
    const d2 = try w2.build();
    defer testing.allocator.free(d2);
    try mock_s3.putObject("so/1/100-live", d2);
    try om.commitStreamObject(11, 1, 100, 150, "so/1/100-live", d2.len);

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    // Large retention so destroy phase does NOT fire this cycle
    cm.mark_destroyed_retention_ms = 999_999_999;

    try cm.runCompaction();

    // Expired SO should be mark_destroyed, not removed
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());
    const expired_so = om.stream_objects.get(10).?;
    try testing.expectEqual(stream_mod.S3ObjectState.mark_destroyed, expired_so.state);

    // Live SO should still be committed
    const live_so = om.stream_objects.get(11).?;
    try testing.expectEqual(stream_mod.S3ObjectState.committed, live_so.state);

    // getObjects should only return the committed (live) one
    const results = try om.getObjects(1, 0, 300, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 100), results[0].start_offset);
}

test "CompactionManager cleanupDestroyed deletes after retention period" {
    // Verify that mark_destroyed objects are NOT deleted before retention
    // expires, and ARE deleted after.
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    var w1 = ObjectWriter.init(testing.allocator);
    defer w1.deinit();
    try w1.addDataBlock(1, 0, 50, 50, "data");
    const d1 = try w1.build();
    defer testing.allocator.free(d1);
    try mock_s3.putObject("so/1/0-10", d1);
    try om.commitStreamObject(10, 1, 0, 50, "so/1/0-10", d1.len);

    // Mark destroyed with a known timestamp
    om.markDestroyedAt(10, 1000);

    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    // 10 second retention for test
    cm.mark_destroyed_retention_ms = 10_000;

    // Try to collect at time 5000 (only 4 seconds elapsed) — too early
    const keys_early = try om.collectDestroyedObjectsAt(10_000, testing.allocator, 5000);
    defer testing.allocator.free(keys_early);
    try testing.expectEqual(@as(usize, 0), keys_early.len);
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());

    // At time 12000 (11 seconds elapsed) — should be ready
    const keys = try om.collectDestroyedObjectsAt(10_000, testing.allocator, 12000);
    defer {
        for (keys) |k| testing.allocator.free(k);
        testing.allocator.free(keys);
    }
    try testing.expectEqual(@as(usize, 1), keys.len);
    try testing.expectEqualStrings("so/1/0-10", keys[0]);
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());
}

test "CompactionManager lifecycle end-to-end: mark → retain → destroy" {
    // Full lifecycle: committed → cleanupExpired marks destroyed → retention
    // period → cleanupDestroyed physically deletes.
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    const stream = try om.createStreamWithId(1, 0);
    stream.advanceEndOffset(200);
    stream.trim(100);

    var w1 = ObjectWriter.init(testing.allocator);
    defer w1.deinit();
    try w1.addDataBlock(1, 0, 50, 50, "expired-data");
    const d1 = try w1.build();
    defer testing.allocator.free(d1);
    try mock_s3.putObject("so/1/0-expired", d1);
    try om.commitStreamObject(10, 1, 0, 50, "so/1/0-expired", d1.len);

    var w2 = ObjectWriter.init(testing.allocator);
    defer w2.deinit();
    try w2.addDataBlock(1, 100, 50, 50, "live-data");
    const d2 = try w2.build();
    defer testing.allocator.free(d2);
    try mock_s3.putObject("so/1/100-live", d2);
    try om.commitStreamObject(11, 1, 100, 150, "so/1/100-live", d2.len);

    // Cycle 1: mark destroyed (with large retention so destroy doesn't fire)
    var cm = CompactionManager.init(testing.allocator, &om);
    defer cm.deinit();
    cm.setMockS3(&mock_s3);
    cm.mark_destroyed_retention_ms = 999_999_999;

    try cm.runCompaction();

    // Expired SO is mark_destroyed but still in ObjectManager
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());
    try testing.expectEqual(@as(u64, 0), cm.total_destroyed);

    // Cycle 2: now set retention to 0 so destroy fires immediately
    cm.mark_destroyed_retention_ms = 0;
    cm.last_compaction_ms = 0;

    try cm.runCompaction();

    // Expired SO should now be physically gone
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());
    try testing.expectEqual(@as(u64, 1), cm.total_destroyed);

    // Only live SO remains
    const results = try om.getObjects(1, 0, 300, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 100), results[0].start_offset);
}

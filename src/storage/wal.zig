const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = @import("fs_compat");
const crc32c = @import("core").crc32c;
const log = std.log.scoped(.wal);
const ObjectWriter = @import("s3.zig").ObjectWriter;
const stream_mod = @import("stream.zig");
const ObjectManager = stream_mod.ObjectManager;
const StreamOffsetRange = stream_mod.StreamOffsetRange;

/// Write-Ahead Log (WAL) for durable local writes.
///
/// Records are written to the WAL before being acknowledged to producers.
/// The WAL provides crash recovery — on restart, all committed records
/// can be replayed from the WAL.
///
/// WAL Record Format:
/// ```
///   magic: u8        (0x01)
///   crc: u32         (CRC-32C of record_len + data)
///   record_len: u32  (length of data)
///   data: [record_len]u8
/// ```
///
/// Total overhead per record: 9 bytes (1 magic + 4 crc + 4 len)
pub const Wal = struct {
    const MAGIC: u8 = 0x01;
    const HEADER_SIZE: usize = 9; // magic(1) + crc(4) + len(4)

    dir_path: []const u8,
    allocator: Allocator,
    /// Active segment file being written to (rotated when segment_max_size is exceeded).
    current_segment: ?Segment = null,
    /// Maximum bytes per segment file before rotating to a new one.
    segment_max_size: usize,
    total_bytes_written: u64 = 0,
    total_records_written: u64 = 0,
    /// Metadata for all closed (immutable) segments.
    segments: std.array_list.Managed(SegmentMeta),
    fsync_policy: FsyncPolicy = .every_n_records,
    /// Number of records between fsync calls (only used with every_n_records policy).
    fsync_interval: u64 = 100,
    /// Next segment id to allocate. Initialized from existing WAL files on open.
    next_segment_id: u64 = 0,

    pub const FsyncPolicy = enum {
        /// Fsync after every record write (safest, slowest)
        every_record,
        /// Fsync after every N records
        every_n_records,
        /// Fsync only when explicitly called (fastest, risk of data loss)
        manual,
        /// Fsync periodically based on time (not implemented yet)
        periodic,
    };

    const SegmentMeta = struct {
        id: u64,
        size: u64,
        record_count: u64,
        path: []const u8,
    };

    const Segment = struct {
        file: fs.File,
        id: u64,
        size: u64 = 0,
        record_count: u64 = 0,
    };

    /// Initialize a new WAL in the given directory.
    pub fn init(alloc: Allocator, dir_path: []const u8, segment_max_size: usize) Wal {
        return .{
            .dir_path = dir_path,
            .allocator = alloc,
            .segment_max_size = segment_max_size,
            .segments = std.array_list.Managed(SegmentMeta).init(alloc),
        };
    }

    pub fn deinit(self: *Wal) void {
        if (self.current_segment) |*seg| {
            seg.file.close();
        }
        for (self.segments.items) |meta| {
            self.allocator.free(meta.path);
        }
        self.segments.deinit();
    }

    /// Open or create the WAL directory and prepare for writing.
    pub fn open(self: *Wal) !void {
        // Create directory if it doesn't exist
        fs.makeDirAbsolute(self.dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        // Preserve existing segments for recovery, then append future writes to
        // a fresh segment. Reusing segment 0 with truncate would destroy
        // acknowledged records before recover() can replay them.
        try self.loadExistingSegments();
        try self.openSegment(self.next_segment_id);
        self.next_segment_id += 1;
    }

    /// Append a record to the WAL. Returns the byte offset of the record.
    pub fn append(self: *Wal, data: []const u8) !u64 {
        if (self.current_segment == null) {
            try self.rollSegment();
        }

        var seg = &self.current_segment.?;

        // Check if we need to roll to a new segment
        if (seg.size + HEADER_SIZE + data.len > self.segment_max_size) {
            try self.rollSegment();
            seg = &self.current_segment.?;
        }

        const offset = self.total_bytes_written;

        // Build header
        var header: [HEADER_SIZE]u8 = undefined;
        header[0] = MAGIC;

        // CRC covers record_len + data
        var crc_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_buf, @intCast(data.len), .big);
        var crc_val = crc32c.update(0xFFFFFFFF, &crc_buf);
        crc_val = crc32c.update(crc_val, data);
        crc_val ^= 0xFFFFFFFF;

        std.mem.writeInt(u32, header[1..5], crc_val, .big);
        std.mem.writeInt(u32, header[5..9], @intCast(data.len), .big);

        // Write header + data
        try seg.file.writeAll(&header);
        try seg.file.writeAll(data);

        seg.size += HEADER_SIZE + data.len;
        seg.record_count += 1;
        self.total_bytes_written += HEADER_SIZE + data.len;
        self.total_records_written += 1;

        // Auto-fsync based on policy
        if (self.fsync_policy == .every_record) {
            try seg.file.sync();
        } else if (self.fsync_policy == .every_n_records) {
            if (self.total_records_written % self.fsync_interval == 0) {
                try seg.file.sync();
            }
        }

        return offset;
    }

    /// Append and fsync to ensure durability.
    pub fn appendSync(self: *Wal, data: []const u8) !u64 {
        const offset = try self.append(data);
        try self.sync();
        return offset;
    }

    /// Fsync the current segment.
    pub fn sync(self: *Wal) !void {
        if (self.current_segment) |seg| {
            try seg.file.sync();
        }
    }

    fn rollSegment(self: *Wal) !void {
        // Close current segment
        if (self.current_segment) |*seg| {
            seg.file.close();
            // Record metadata
            const path = try std.fmt.allocPrint(self.allocator, "{s}/wal-{d:0>20}.log", .{ self.dir_path, seg.id });
            errdefer self.allocator.free(path);
            try self.segments.append(.{
                .id = seg.id,
                .size = seg.size,
                .record_count = seg.record_count,
                .path = path,
            });
        }

        try self.openSegment(self.next_segment_id);
        self.next_segment_id += 1;
    }

    fn openSegment(self: *Wal, seg_id: u64) !void {
        const filename = try std.fmt.allocPrint(self.allocator, "{s}/wal-{d:0>20}.log", .{ self.dir_path, seg_id });
        defer self.allocator.free(filename);

        const file = try fs.createFileAbsolute(filename, .{
            .truncate = true,
        });

        self.current_segment = .{
            .file = file,
            .id = seg_id,
        };
    }

    fn loadExistingSegments(self: *Wal) !void {
        var dir = try fs.openDirAbsolute(self.dir_path, .{ .iterate = true });
        defer dir.close();

        var max_id: ?u64 = null;
        var total_size: u64 = 0;

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.name, "wal-") or !std.mem.endsWith(u8, entry.name, ".log")) {
                continue;
            }
            const id_part = entry.name[4 .. entry.name.len - 4];
            const id = std.fmt.parseInt(u64, id_part, 10) catch continue;

            const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.dir_path, entry.name });
            errdefer self.allocator.free(path);

            const file = fs.openFileAbsolute(path, .{}) catch {
                self.allocator.free(path);
                continue;
            };
            const stat = file.stat() catch {
                file.close();
                self.allocator.free(path);
                continue;
            };
            file.close();

            try self.segments.append(.{
                .id = id,
                .size = stat.size,
                .record_count = 0,
                .path = path,
            });
            total_size += stat.size;
            if (max_id == null or id > max_id.?) max_id = id;
        }

        std.mem.sort(SegmentMeta, self.segments.items, {}, struct {
            fn lessThan(_: void, a: SegmentMeta, b: SegmentMeta) bool {
                return a.id < b.id;
            }
        }.lessThan);

        self.total_bytes_written = total_size;
        self.next_segment_id = if (max_id) |id| id + 1 else 0;
    }

    /// Remove old WAL segments that have been flushed to S3.
    /// Keeps the current active segment and removes all completed segments
    /// with ID <= `up_to_segment_id`.
    pub fn cleanupSegments(self: *Wal, up_to_segment_id: u64) !u64 {
        var removed: u64 = 0;
        var i: usize = 0;
        while (i < self.segments.items.len) {
            const meta = &self.segments.items[i];
            if (meta.id <= up_to_segment_id) {
                // Delete the file
                fs.deleteFileAbsolute(meta.path) catch |err| {
                    log.warn("Failed to delete WAL segment {s}: {}", .{ meta.path, err });
                };
                self.allocator.free(meta.path);
                _ = self.segments.orderedRemove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }
        return removed;
    }

    /// Get the total number of segments (including current).
    pub fn segmentCount(self: *const Wal) usize {
        return self.segments.items.len + @as(usize, if (self.current_segment != null) 1 else 0);
    }

    /// Recover records from the WAL directory.
    /// Calls `callback` for each valid record found.
    pub fn recover(self: *Wal, callback: *const fn (data: []const u8, offset: u64) void) !u64 {
        return self.recoverWithContext(callback, struct {
            fn cb(cb_fn: *const fn (data: []const u8, offset: u64) void, data: []const u8, offset: u64) !void {
                cb_fn(data, offset);
            }
        }.cb);
    }

    pub fn recoverWithContext(
        self: *Wal,
        context: anytype,
        comptime callback: fn (@TypeOf(context), data: []const u8, offset: u64) anyerror!void,
    ) !u64 {
        log.info("Starting WAL recovery from {s}", .{self.dir_path});
        var dir = try fs.openDirAbsolute(self.dir_path, .{ .iterate = true });
        defer dir.close();

        var recovered: u64 = 0;
        var total_offset: u64 = 0;

        // Collect and sort WAL files
        var files = std.array_list.Managed([]const u8).init(self.allocator);
        defer {
            for (files.items) |f| self.allocator.free(f);
            files.deinit();
        }

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (std.mem.startsWith(u8, entry.name, "wal-") and std.mem.endsWith(u8, entry.name, ".log")) {
                const name_copy = try self.allocator.dupe(u8, entry.name);
                try files.append(name_copy);
            }
        }

        // Sort by name (which is by segment ID due to zero-padding)
        std.mem.sort([]const u8, files.items, {}, struct {
            fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lessThan);

        // Read each file
        for (files.items) |filename| {
            const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.dir_path, filename });
            defer self.allocator.free(path);

            const file = fs.openFileAbsolute(path, .{}) catch continue;
            defer file.close();

            const content = file.readToEndAlloc(self.allocator, 1024 * 1024 * 1024) catch continue;
            defer self.allocator.free(content);

            var pos: usize = 0;
            while (pos + HEADER_SIZE <= content.len) {
                // Validate magic
                if (content[pos] != MAGIC) break;

                const stored_crc = std.mem.readInt(u32, content[pos + 1 ..][0..4], .big);
                const record_len = std.mem.readInt(u32, content[pos + 5 ..][0..4], .big);

                if (pos + HEADER_SIZE + record_len > content.len) break;

                // Validate CRC
                var crc_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &crc_buf, record_len, .big);
                var computed_crc = crc32c.update(0xFFFFFFFF, &crc_buf);
                computed_crc = crc32c.update(computed_crc, content[pos + HEADER_SIZE .. pos + HEADER_SIZE + record_len]);
                computed_crc ^= 0xFFFFFFFF;

                if (computed_crc != stored_crc) {
                    log.warn("WAL corruption detected: CRC mismatch at offset {d} in {s}", .{ pos, filename });
                    break; // Corruption — stop here
                }

                const data = content[pos + HEADER_SIZE .. pos + HEADER_SIZE + record_len];
                try callback(context, data, total_offset);

                pos += HEADER_SIZE + record_len;
                total_offset += HEADER_SIZE + record_len;
                recovered += 1;
            }
        }

        log.info("WAL recovery complete: {d} records recovered", .{recovered});
        return recovered;
    }
};

/// In-memory WAL for testing (no filesystem dependency).
pub const MemoryWal = struct {
    records: std.array_list.Managed(Record),
    allocator: Allocator,
    total_bytes: u64 = 0,

    const Record = struct {
        offset: u64,
        data: []u8,
    };

    pub fn init(alloc: Allocator) MemoryWal {
        return .{
            .records = std.array_list.Managed(Record).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *MemoryWal) void {
        for (self.records.items) |rec| {
            self.allocator.free(rec.data);
        }
        self.records.deinit();
    }

    pub fn append(self: *MemoryWal, data: []const u8) !u64 {
        const offset = self.total_bytes;
        const data_copy = try self.allocator.dupe(u8, data);
        try self.records.append(.{ .offset = offset, .data = data_copy });
        self.total_bytes += data.len;
        return offset;
    }

    pub fn recordCount(self: *const MemoryWal) usize {
        return self.records.items.len;
    }

    /// Get a record by index.
    pub fn get(self: *const MemoryWal, index: usize) ?[]const u8 {
        if (index >= self.records.items.len) return null;
        return self.records.items[index].data;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "MemoryWal basic append and read" {
    var wal = MemoryWal.init(testing.allocator);
    defer wal.deinit();

    const offset1 = try wal.append("hello");
    const offset2 = try wal.append("world");

    try testing.expectEqual(@as(u64, 0), offset1);
    try testing.expectEqual(@as(u64, 5), offset2);
    try testing.expectEqual(@as(usize, 2), wal.recordCount());
    try testing.expectEqualStrings("hello", wal.get(0).?);
    try testing.expectEqualStrings("world", wal.get(1).?);
    try testing.expect(wal.get(2) == null);
}

test "Wal CRC header format" {
    // Verify the header format manually
    const data = "test record data";
    var header: [Wal.HEADER_SIZE]u8 = undefined;
    header[0] = Wal.MAGIC;

    var crc_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc_buf, @intCast(data.len), .big);
    var crc_val = crc32c.update(0xFFFFFFFF, &crc_buf);
    crc_val = crc32c.update(crc_val, data);
    crc_val ^= 0xFFFFFFFF;

    std.mem.writeInt(u32, header[1..5], crc_val, .big);
    std.mem.writeInt(u32, header[5..9], @intCast(data.len), .big);

    try testing.expectEqual(Wal.MAGIC, header[0]);
    try testing.expectEqual(@as(u32, @intCast(data.len)), std.mem.readInt(u32, header[5..9], .big));
}

test "Wal filesystem append and recover" {
    const tmp_dir = "/tmp/automq-wal-test";

    // Clean up first
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Write records
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();

        _ = try wal.append("record-1");
        _ = try wal.append("record-2");
        _ = try wal.append("record-3");
        try wal.sync();

        try testing.expectEqual(@as(u64, 3), wal.total_records_written);
    }

    // Recover records
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();

        const count = try wal.recover(&struct {
            fn cb(_: []const u8, _: u64) void {
                // Can't easily increment a counter here in Zig without globals
                // but the return value tells us the count
            }
        }.cb);

        try testing.expectEqual(@as(u64, 3), count);
    }

    // Clean up
    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal open preserves existing segments for recovery" {
    const tmp_dir = "/tmp/automq-wal-open-preserve-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();
        _ = try wal.append("record-before-restart");
        try wal.sync();
    }

    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();

        const count = try wal.recover(&struct {
            fn cb(_: []const u8, _: u64) void {}
        }.cb);
        try testing.expectEqual(@as(u64, 1), count);
        try testing.expect(wal.segmentCount() >= 2);
    }
}

test "Wal CRC corruption detection during recovery" {
    const tmp_dir = "/tmp/automq-wal-crc-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Write some valid records
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();

        _ = try wal.append("good-record-1");
        _ = try wal.append("good-record-2");
        try wal.sync();
    }

    // Corrupt the second record's CRC
    {
        const seg_path = try std.fmt.allocPrint(testing.allocator, "{s}/wal-{d:0>20}.log", .{ tmp_dir, @as(u64, 0) });
        defer testing.allocator.free(seg_path);

        const file = try fs.openFileAbsolute(seg_path, .{ .mode = .read_write });
        defer file.close();

        // First record: HEADER_SIZE(9) + "good-record-1"(13) = 22 bytes
        // Second record starts at offset 22. CRC is at bytes 23..27 (offset+1..offset+5)
        const corrupt_offset: u64 = 22 + 1; // CRC position of second record
        try file.seekTo(corrupt_offset);
        const corrupt_byte = [_]u8{0xFF};
        try file.writeAll(&corrupt_byte);
    }

    // Recovery should find only the first valid record
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();

        var recovered_data = std.array_list.Managed([]const u8).init(testing.allocator);
        defer recovered_data.deinit();

        const count = try wal.recover(&struct {
            fn cb(_: []const u8, _: u64) void {}
        }.cb);

        // Only the first record should be recovered (corruption stops parsing)
        try testing.expectEqual(@as(u64, 1), count);
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal segment rollover" {
    const tmp_dir = "/tmp/automq-wal-roll-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Use a very small segment size to force rollover
    const small_segment = 50; // 50 bytes — a single record with overhead will fill this
    {
        var wal = Wal.init(testing.allocator, tmp_dir, small_segment);
        defer wal.deinit();
        try wal.open();

        // Each record: 9 (header) + data_len bytes
        // "record-A" is 8 bytes, total 17 bytes per record
        // Segment size 50, so ~2 records before rollover
        _ = try wal.append("record-A"); // 17 bytes
        _ = try wal.append("record-B"); // 34 bytes
        _ = try wal.append("record-C"); // Would exceed 50, triggers rollover

        try testing.expect(wal.segmentCount() >= 2);
        try testing.expectEqual(@as(u64, 3), wal.total_records_written);
    }

    // Recover all records across segments
    {
        var wal = Wal.init(testing.allocator, tmp_dir, small_segment);
        defer wal.deinit();

        const count = try wal.recover(&struct {
            fn cb(_: []const u8, _: u64) void {}
        }.cb);

        try testing.expectEqual(@as(u64, 3), count);
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal magic byte validation in recovery" {
    const tmp_dir = "/tmp/automq-wal-magic-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Write valid records
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();
        _ = try wal.append("test-data");
        try wal.sync();
    }

    // Corrupt magic byte of first record
    {
        const seg_path = try std.fmt.allocPrint(testing.allocator, "{s}/wal-{d:0>20}.log", .{ tmp_dir, @as(u64, 0) });
        defer testing.allocator.free(seg_path);

        const file = try fs.openFileAbsolute(seg_path, .{ .mode = .read_write });
        defer file.close();

        try file.seekTo(0);
        const bad_magic = [_]u8{0xFF}; // Not 0x01
        try file.writeAll(&bad_magic);
    }

    // Recovery should find zero valid records
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();

        const count = try wal.recover(&struct {
            fn cb(_: []const u8, _: u64) void {}
        }.cb);

        try testing.expectEqual(@as(u64, 0), count);
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal append returns monotonically increasing offsets" {
    const tmp_dir = "/tmp/automq-wal-offset-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
    defer wal.deinit();
    try wal.open();

    const o1 = try wal.append("data1");
    const o2 = try wal.append("data2");
    const o3 = try wal.append("data3");

    try testing.expect(o2 > o1);
    try testing.expect(o3 > o2);

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal empty recovery on fresh directory" {
    const tmp_dir = "/tmp/automq-wal-empty-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};

    var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
    defer wal.deinit();

    const count = try wal.recover(&struct {
        fn cb(_: []const u8, _: u64) void {}
    }.cb);

    try testing.expectEqual(@as(u64, 0), count);

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

// ---------------------------------------------------------------
// S3 WAL Batcher — async batching for S3 WAL mode
// ---------------------------------------------------------------

/// S3WalBatcher accumulates records into a buffer and uploads them to S3 in
/// batches, rather than doing a synchronous S3 PUT per produce.
///
/// ZMQ batches WAL writes into "Bulk" objects uploaded asynchronously,
/// following the same approach as AutoMQ's WAL batching.
/// This implementation provides the same semantics:
/// - Accumulates records in a buffer (max 4MB or 250ms flush interval)
/// - When threshold reached, uploads the batch to S3 as a single object
/// - Tracks batch upload count for metrics
///
/// Write path durability:
/// The batcher now tracks a `last_flushed_offset` so that produce() can
/// verify data has reached S3 before acking. In sync mode, flushNow()
/// is called directly from produce() to ensure durability. In async mode,
/// flushes happen from the broker tick() (fast but lossy).
///
/// Since the Zig broker is single-threaded (epoll loop), flushes are triggered
/// by the broker tick() rather than a background thread.
pub const S3WalBatcher = struct {
    /// Pending record entries waiting to be flushed to S3.
    buffer: std.array_list.Managed(BatchEntry),
    /// Total bytes currently buffered (used for threshold-based flushing).
    buffer_size: usize = 0,
    /// Timestamp of the last successful flush (for interval-based flushing).
    last_flush_ms: i64,
    /// Number of successful batch uploads to S3.
    batch_upload_count: u64 = 0,
    /// Number of failed batch uploads to S3.
    batch_upload_failures: u64 = 0,
    /// Monotonic counter for generating unique S3 object keys.
    s3_object_counter: u64 = 0,
    allocator: Allocator,
    /// Track the highest offset that has been durably flushed to S3.
    last_flushed_offset: u64 = 0,
    /// Track the highest offset appended to the buffer.
    last_appended_offset: u64 = 0,
    /// Configurable batch size (default 4MB).
    max_batch_size: usize = 4 * 1024 * 1024,
    /// Configurable flush interval (default 250ms).
    flush_interval_ms: i64 = 250,
    /// WAL flush mode — sync = flush after every produce (durable),
    /// async = batch flush from tick (fast but lossy). Default: sync.
    flush_mode: WalFlushMode = .sync,
    /// Fencing: WAL epoch — only the current epoch can write to S3.
    /// On failover, the new owner bumps the epoch, fencing the old writer.
    wal_epoch: u64 = 0,
    /// Fencing: set to true when this batcher has been fenced (epoch bumped by someone else).
    /// Once fenced, all writes are rejected with error.WalFenced.
    is_fenced: bool = false,
    /// Optional: ObjectManager for registering flushed objects as StreamSetObjects.
    /// When set, flushNow() will register the flushed object with the ObjectManager.
    object_manager: ?*ObjectManager = null,
    /// Tracks the S3 object counter value at the time of the last successful
    /// S3 upload. Used by the broker tick() to determine which local WAL
    /// segments can be safely deleted — segments with ID < last_flushed_segment_id
    /// have been durably flushed to S3 and are no longer needed for recovery.
    ///
    /// NOTE: AutoMQ trims WAL after S3 upload confirmation in
    /// S3Storage.commitStreamSetObject(). ZMQ tracks the counter here
    /// and defers cleanup to tick() since the single-threaded model makes
    /// WAL segment deletion non-urgent.
    last_flushed_segment_id: u64 = 0,
    /// Group commit: partitions with unflushed data. stream_id → next_offset to set as HW.
    /// Populated by trackPendingHW(), drained after successful flush by drainPendingHWUpdates().
    pending_hw_updates: std.AutoHashMap(u64, u64),
    /// Group commit: number of produce requests waiting on the current batch.
    pending_produce_count: u32 = 0,

    pub const BatchEntry = struct {
        stream_id: u64,
        base_offset: u64,
        data: []u8, // owned copy of the record data

        pub fn deinit(self: *BatchEntry, alloc: Allocator) void {
            alloc.free(self.data);
        }
    };

    pub fn init(alloc: Allocator) S3WalBatcher {
        return .{
            .buffer = std.array_list.Managed(BatchEntry).init(alloc),
            .last_flush_ms = @import("time_compat").milliTimestamp(),
            .allocator = alloc,
            .pending_hw_updates = std.AutoHashMap(u64, u64).init(alloc),
        };
    }

    /// Initialize with configuration.
    pub fn initWithConfig(alloc: Allocator, config: S3WalConfig) S3WalBatcher {
        return .{
            .buffer = std.array_list.Managed(BatchEntry).init(alloc),
            .last_flush_ms = @import("time_compat").milliTimestamp(),
            .allocator = alloc,
            .max_batch_size = config.batch_size,
            .flush_interval_ms = config.flush_interval_ms,
            .flush_mode = config.flush_mode,
            .pending_hw_updates = std.AutoHashMap(u64, u64).init(alloc),
        };
    }

    pub fn deinit(self: *S3WalBatcher) void {
        for (self.buffer.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.buffer.deinit();
        self.pending_hw_updates.deinit();
    }

    /// Append records to the batcher's buffer.
    /// Tracks the appended offset so flushNow() can verify durability.
    pub fn append(self: *S3WalBatcher, stream_id: u64, base_offset: u64, records: []const u8) !void {
        // Fencing check: reject writes if this batcher has been fenced
        if (self.is_fenced) {
            log.debug("S3WAL append rejected: batcher is fenced (epoch={d})", .{self.wal_epoch});
            return error.WalFenced;
        }

        const data_copy = try self.allocator.dupe(u8, records);
        try self.buffer.append(.{
            .stream_id = stream_id,
            .base_offset = base_offset,
            .data = data_copy,
        });
        self.buffer_size += records.len;
        const next_offset = base_offset + recordCountFromData(records);
        if (next_offset > self.last_appended_offset) {
            self.last_appended_offset = next_offset;
        }
    }

    /// Check if the batch should be flushed (size or time threshold).
    /// Called from the broker's periodic tick.
    pub fn shouldFlush(self: *const S3WalBatcher) bool {
        if (self.buffer.items.len == 0) return false;
        const now = @import("time_compat").milliTimestamp();
        const time_elapsed = now - self.last_flush_ms;
        return self.buffer_size >= self.max_batch_size or time_elapsed >= self.flush_interval_ms;
    }

    /// Group commit: record that a partition needs HW advancement after the next flush.
    /// Called from produce() in group_commit mode.
    pub fn trackPendingHW(self: *S3WalBatcher, stream_id: u64, next_offset: u64) !void {
        const gop = try self.pending_hw_updates.getOrPut(stream_id);
        if (!gop.found_existing or next_offset > gop.value_ptr.*) {
            gop.value_ptr.* = next_offset;
        }
        self.pending_produce_count += 1;
    }

    /// Group commit: return pending HW updates after a successful flush, and reset tracking.
    /// Caller is responsible for deiniting the returned map.
    pub fn drainPendingHWUpdates(self: *S3WalBatcher) std.AutoHashMap(u64, u64) {
        const result = self.pending_hw_updates;
        self.pending_hw_updates = std.AutoHashMap(u64, u64).init(self.allocator);
        self.pending_produce_count = 0;
        return result;
    }

    /// Group commit: returns true if there are produces waiting for an S3 flush.
    pub fn hasPendingFlush(self: *const S3WalBatcher) bool {
        return self.pending_produce_count > 0;
    }

    fn recordCountFromData(data: []const u8) u64 {
        if (data.len >= 61 and data[16] == 2) {
            const rc = std.mem.readInt(i32, data[57..61], .big);
            if (rc > 0) return @intCast(rc);
        }
        return 1;
    }

    fn buildBatchData(self: *S3WalBatcher) !?[]u8 {
        if (self.buffer.items.len == 0) return null;

        // Build S3 object using ObjectWriter format (indexed, with footer)
        // so the resulting object is a proper StreamSetObject that can be
        // parsed by ObjectReader during fetch and compaction.
        var writer = ObjectWriter.init(self.allocator);
        defer writer.deinit();

        for (self.buffer.items) |entry| {
            // Parse record_count from RecordBatch header if possible (V2 format)
            const record_count: u32 = @intCast(recordCountFromData(entry.data));

            try writer.addDataBlock(
                entry.stream_id,
                entry.base_offset,
                record_count, // end_offset_delta
                record_count,
                entry.data,
            );
        }

        return try writer.build();
    }

    fn markFlushSuccess(self: *S3WalBatcher) void {
        // Clear the buffer
        for (self.buffer.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.buffer.clearRetainingCapacity();
        self.buffer_size = 0;
        self.last_flush_ms = @import("time_compat").milliTimestamp();
        self.batch_upload_count += 1;
        self.s3_object_counter += 1;

        // Update last_flushed_offset to mark data as durable
        self.last_flushed_offset = self.last_appended_offset;
        self.last_flushed_segment_id = self.s3_object_counter;
    }

    /// Force flush all buffered entries to an S3 object payload.
    /// Returns ObjectWriter bytes and marks the current buffer flushed.
    ///
    /// This helper is used by unit tests and offline object construction. The
    /// production upload path uses flushNow(), which marks success only after
    /// the S3 PUT has completed.
    pub fn flushBuild(self: *S3WalBatcher) !?[]u8 {
        const batch_data = try self.buildBatchData();
        if (batch_data != null) self.markFlushSuccess();

        return batch_data;
    }

    /// Synchronous flush — build batch data and upload to S3 immediately.
    /// Called directly from produce() in sync mode to ensure durability before acking.
    /// Returns true if flush succeeded (data is durable in S3).
    pub fn flushNow(self: *S3WalBatcher, s3_storage: anytype) bool {
        // Fencing check: reject flushes if this batcher has been fenced
        if (self.is_fenced) {
            log.warn("S3 WAL flush rejected: batcher is fenced (epoch={d})", .{self.wal_epoch});
            return false;
        }

        // Compute stream ranges BEFORE flushBuild clears the buffer
        const ranges = if (self.object_manager != null)
            self.computeStreamRanges() catch null
        else
            null;
        defer if (ranges) |r| self.allocator.free(r);

        const batch_data = self.buildBatchData() catch |err| {
            log.warn("S3 WAL sync flush build failed: {}", .{err});
            self.batch_upload_failures += 1;
            return false;
        };

        if (batch_data) |data| {
            defer self.allocator.free(data);

            // Include epoch in S3 object key for fencing traceability
            const obj_key = self.nextObjectKey(self.allocator) catch {
                self.batch_upload_failures += 1;
                return false;
            };
            defer self.allocator.free(obj_key);

            s3_storage.putObject(obj_key, data) catch |err| {
                log.warn("S3 WAL sync flush upload failed: {}", .{err});
                self.batch_upload_failures += 1;
                return false;
            };

            self.markFlushSuccess();

            // Register as StreamSetObject in ObjectManager
            if (self.object_manager) |om| {
                if (ranges) |r| {
                    const obj_id = om.allocateObjectId();
                    const order_id = om.next_order_id;
                    om.next_order_id += 1;
                    om.commitStreamSetObject(
                        obj_id,
                        om.node_id,
                        order_id,
                        r,
                        obj_key,
                        data.len,
                    ) catch |err| {
                        log.warn("Failed to register SSO with ObjectManager: {}", .{err});
                    };
                }
            }

            return true; // Data is now durable in S3
        }

        return true; // Nothing to flush — consider it "durable"
    }

    /// Check if data up to the given offset has been flushed to S3.
    pub fn isFlushed(self: *const S3WalBatcher, offset: u64) bool {
        return self.last_flushed_offset >= offset;
    }

    /// Fence this batcher — called when failover detects this node should stop writing.
    /// All subsequent append() and flushNow() calls will be rejected.
    pub fn fence(self: *S3WalBatcher) void {
        self.is_fenced = true;
        log.info("S3 WAL batcher fenced at epoch {d}", .{self.wal_epoch});
    }

    /// Update the WAL epoch — called when this node becomes the new owner after failover.
    pub fn setEpoch(self: *S3WalBatcher, new_epoch: u64) void {
        log.info("S3 WAL epoch updated: {d} -> {d}", .{ self.wal_epoch, new_epoch });
        self.wal_epoch = new_epoch;
        self.is_fenced = false; // Un-fence with new epoch
    }

    /// Get the S3 object key for the current batch (includes epoch for fencing).
    pub fn currentObjectKey(self: *const S3WalBatcher, alloc: Allocator) ![]u8 {
        return try std.fmt.allocPrint(alloc, "wal/epoch-{d}/bulk/{d:0>10}", .{ self.wal_epoch, self.s3_object_counter });
    }

    fn nextObjectKey(self: *const S3WalBatcher, alloc: Allocator) ![]u8 {
        return try std.fmt.allocPrint(alloc, "wal/epoch-{d}/bulk/{d:0>10}", .{ self.wal_epoch, self.s3_object_counter + 1 });
    }

    /// Number of entries currently buffered.
    pub fn pendingCount(self: *const S3WalBatcher) usize {
        return self.buffer.items.len;
    }

    /// Total bytes currently buffered.
    pub fn pendingBytes(self: *const S3WalBatcher) usize {
        return self.buffer_size;
    }

    /// Set the ObjectManager for registering flushed objects as StreamSetObjects.
    pub fn setObjectManager(self: *S3WalBatcher, om: *ObjectManager) void {
        self.object_manager = om;
    }

    /// Compute per-stream offset ranges from the current buffer entries.
    /// Used to build StreamSetObject metadata before flush.
    /// Caller owns the returned slice.
    pub fn computeStreamRanges(self: *const S3WalBatcher) ![]StreamOffsetRange {
        var range_map = std.AutoHashMap(u64, StreamOffsetRange).init(self.allocator);
        defer range_map.deinit();

        for (self.buffer.items) |entry| {
            // Parse record_count from RecordBatch header if possible
            const record_count = recordCountFromData(entry.data);

            const end_offset = entry.base_offset + record_count;
            var gop = try range_map.getOrPut(entry.stream_id);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{
                    .stream_id = entry.stream_id,
                    .start_offset = entry.base_offset,
                    .end_offset = end_offset,
                };
            } else {
                gop.value_ptr.start_offset = @min(gop.value_ptr.start_offset, entry.base_offset);
                gop.value_ptr.end_offset = @max(gop.value_ptr.end_offset, end_offset);
            }
        }

        var result = try self.allocator.alloc(StreamOffsetRange, range_map.count());
        var i: usize = 0;
        var it = range_map.valueIterator();
        while (it.next()) |v| {
            result[i] = v.*;
            i += 1;
        }
        return result;
    }
};

/// WAL flush mode configuration.
/// sync = flush after every produce (durable, matches Java Kafka behavior)
/// async = batch flush from tick (fast but may lose data on crash)
/// group_commit = reserved for response-delayed group commit; currently handled
/// like sync by PartitionStore so produce responses are not sent before S3 durability.
pub const WalFlushMode = enum {
    sync,
    async_flush,
    group_commit,
};

/// S3 WAL configuration parameters.
pub const S3WalConfig = struct {
    batch_size: usize = 4 * 1024 * 1024, // 4MB default
    flush_interval_ms: i64 = 250,
    flush_mode: WalFlushMode = .sync,
};

test "S3WalBatcher basic append" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try batcher.append(1, 0, "record-data-1");
    try batcher.append(1, 1, "record-data-2");

    try testing.expectEqual(@as(usize, 2), batcher.pendingCount());
    try testing.expect(batcher.pendingBytes() > 0);
    // last_appended_offset should track highest offset
    try testing.expectEqual(@as(u64, 2), batcher.last_appended_offset);
}

test "S3WalBatcher flush builds ObjectWriter format" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try batcher.append(1, 0, "aaa");
    try batcher.append(1, 1, "bbb");

    const data = try batcher.flushBuild();
    try testing.expect(data != null);
    defer testing.allocator.free(data.?);

    // Output should be valid ObjectWriter format (parseable by ObjectReader)
    const ObjectReader = @import("s3.zig").ObjectReader;
    var reader = try ObjectReader.parse(testing.allocator, data.?);
    defer reader.deinit();

    // Should have 2 data blocks (one per append)
    try testing.expectEqual(@as(usize, 2), reader.index_entries.len);
    // Both blocks should be for stream 1
    try testing.expectEqual(@as(u64, 1), reader.index_entries[0].stream_id);
    try testing.expectEqual(@as(u64, 1), reader.index_entries[1].stream_id);
    // Offsets should be 0 and 1
    try testing.expectEqual(@as(u64, 0), reader.index_entries[0].start_offset);
    try testing.expectEqual(@as(u64, 1), reader.index_entries[1].start_offset);
    // Data should be readable
    const block0 = reader.readBlock(0).?;
    try testing.expectEqualStrings("aaa", block0);
    const block1 = reader.readBlock(1).?;
    try testing.expectEqualStrings("bbb", block1);

    try testing.expectEqual(@as(usize, 0), batcher.pendingCount());
    try testing.expectEqual(@as(u64, 1), batcher.batch_upload_count);
    try testing.expectEqual(@as(u64, 2), batcher.last_flushed_offset);
}

test "S3WalBatcher empty flush returns null" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    const data = try batcher.flushBuild();
    try testing.expect(data == null);
}

test "S3WalBatcher isFlushed tracks durability" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try batcher.append(1, 0, "data1");
    try batcher.append(1, 1, "data2");

    // Not flushed yet
    try testing.expect(!batcher.isFlushed(2));

    const data = try batcher.flushBuild();
    if (data) |d| testing.allocator.free(d);

    // Now flushed
    try testing.expect(batcher.isFlushed(2));
}

test "S3WalBatcher configurable batch size" {
    var batcher = S3WalBatcher.initWithConfig(testing.allocator, .{
        .batch_size = 100,
        .flush_interval_ms = 1000,
        .flush_mode = .sync,
    });
    defer batcher.deinit();

    try testing.expectEqual(@as(usize, 100), batcher.max_batch_size);
    try testing.expectEqual(@as(i64, 1000), batcher.flush_interval_ms);
}

test "S3WalBatcher flushNow with MockS3" {
    const MockS3 = @import("s3.zig").MockS3;

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();
    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Append some data
    try batcher.append(1, 0, "record-a");
    try batcher.append(1, 1, "record-b");

    try testing.expectEqual(@as(usize, 2), batcher.pendingCount());

    // Flush to MockS3
    const flushed = batcher.flushNow(&mock_s3);
    try testing.expect(flushed);

    // Buffer should be empty
    try testing.expectEqual(@as(usize, 0), batcher.pendingCount());
    // MockS3 should have one object
    try testing.expectEqual(@as(usize, 1), mock_s3.objectCount());
    try testing.expectEqual(@as(u64, 1), batcher.batch_upload_count);
    // Durability tracking
    try testing.expect(batcher.isFlushed(2));
}

test "S3WalBatcher flushNow empty returns true" {
    const MockS3 = @import("s3.zig").MockS3;

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();
    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Flush with nothing in buffer
    const flushed = batcher.flushNow(&mock_s3);
    try testing.expect(flushed);
    try testing.expectEqual(@as(usize, 0), mock_s3.objectCount());
}

test "S3WalBatcher flushNow preserves buffer on upload failure" {
    const FailingS3 = struct {
        pub fn putObject(_: *@This(), _: []const u8, _: []const u8) !void {
            return error.InjectedPutFailure;
        }
    };

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();
    var failing_s3 = FailingS3{};

    try batcher.append(1, 0, "record-a");

    const flushed = batcher.flushNow(&failing_s3);
    try testing.expect(!flushed);
    try testing.expectEqual(@as(usize, 1), batcher.pendingCount());
    try testing.expectEqual(@as(u64, 0), batcher.batch_upload_count);
    try testing.expectEqual(@as(u64, 1), batcher.batch_upload_failures);
    try testing.expect(!batcher.isFlushed(1));
}

test "S3WalBatcher fencing rejects appends" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    // Append succeeds before fencing
    try batcher.append(1, 0, "data1");
    try testing.expectEqual(@as(usize, 1), batcher.pendingCount());

    // Fence the batcher
    batcher.fence();
    try testing.expect(batcher.is_fenced);

    // Append fails after fencing
    const result = batcher.append(1, 1, "data2");
    try testing.expectError(error.WalFenced, result);
    try testing.expectEqual(@as(usize, 1), batcher.pendingCount()); // Still 1
}

test "S3WalBatcher fencing rejects flushNow" {
    const MockS3 = @import("s3.zig").MockS3;
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try batcher.append(1, 0, "data");

    // Fence the batcher
    batcher.fence();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // flushNow returns false when fenced
    const flushed = batcher.flushNow(&mock_s3);
    try testing.expect(!flushed);
    try testing.expectEqual(@as(usize, 0), mock_s3.objectCount());
}

test "S3WalBatcher setEpoch un-fences" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    batcher.fence();
    try testing.expect(batcher.is_fenced);

    // Set new epoch un-fences
    batcher.setEpoch(5);
    try testing.expect(!batcher.is_fenced);
    try testing.expectEqual(@as(u64, 5), batcher.wal_epoch);

    // Append succeeds with new epoch
    try batcher.append(1, 0, "data");
    try testing.expectEqual(@as(usize, 1), batcher.pendingCount());
}

test "S3WalBatcher fencing clears on setEpoch then fences again" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    // Fence → un-fence → fence cycle (simulates repeated failover)
    batcher.fence();
    try testing.expect(batcher.is_fenced);

    batcher.setEpoch(10);
    try testing.expect(!batcher.is_fenced);
    try batcher.append(1, 0, "data-epoch-10");

    batcher.fence();
    try testing.expect(batcher.is_fenced);
    try testing.expectError(error.WalFenced, batcher.append(1, 1, "rejected"));

    batcher.setEpoch(20);
    try testing.expect(!batcher.is_fenced);
    try batcher.append(1, 1, "data-epoch-20");
    try testing.expectEqual(@as(usize, 2), batcher.pendingCount());
}

test "S3WalBatcher fenced batcher preserves buffered data" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    // Append data before fencing
    try batcher.append(1, 0, "pre-fence-data");
    try testing.expectEqual(@as(usize, 1), batcher.pendingCount());

    // Fence — should not destroy buffered data
    batcher.fence();
    try testing.expectEqual(@as(usize, 1), batcher.pendingCount());
    try testing.expect(batcher.pendingBytes() > 0);

    // But new appends are rejected
    try testing.expectError(error.WalFenced, batcher.append(1, 1, "rejected"));
    try testing.expectEqual(@as(usize, 1), batcher.pendingCount());
}

test "S3WalBatcher epoch monotonicity" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    batcher.setEpoch(5);
    try testing.expectEqual(@as(u64, 5), batcher.wal_epoch);

    // Setting a lower epoch is allowed (controller decides policy)
    batcher.setEpoch(3);
    try testing.expectEqual(@as(u64, 3), batcher.wal_epoch);
}

test "S3WalBatcher epoch in object key" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    batcher.setEpoch(42);

    const key = try batcher.currentObjectKey(testing.allocator);
    defer testing.allocator.free(key);

    // Key should include epoch
    try testing.expect(std.mem.indexOf(u8, key, "epoch-42") != null);
}

test "S3WalBatcher flushBuild multi-stream produces valid indexed object" {
    const ObjectReader = @import("s3.zig").ObjectReader;
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    // Append data from 3 different streams
    try batcher.append(100, 0, "stream100-block0");
    try batcher.append(200, 0, "stream200-block0");
    try batcher.append(100, 1, "stream100-block1");
    try batcher.append(300, 0, "stream300-block0");

    const data = try batcher.flushBuild();
    try testing.expect(data != null);
    defer testing.allocator.free(data.?);

    var reader = try ObjectReader.parse(testing.allocator, data.?);
    defer reader.deinit();

    // Should have 4 data blocks total
    try testing.expectEqual(@as(usize, 4), reader.index_entries.len);

    // Verify each block's data is correct by reading directly
    try testing.expectEqualStrings("stream100-block0", reader.readBlock(0).?);
    try testing.expectEqualStrings("stream200-block0", reader.readBlock(1).?);
    try testing.expectEqualStrings("stream100-block1", reader.readBlock(2).?);
    try testing.expectEqualStrings("stream300-block0", reader.readBlock(3).?);

    // Verify stream IDs in index
    try testing.expectEqual(@as(u64, 100), reader.index_entries[0].stream_id);
    try testing.expectEqual(@as(u64, 200), reader.index_entries[1].stream_id);
    try testing.expectEqual(@as(u64, 100), reader.index_entries[2].stream_id);
    try testing.expectEqual(@as(u64, 300), reader.index_entries[3].stream_id);
}

test "S3WalBatcher computeStreamRanges multi-stream" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    // Stream 1: offsets 0, 1, 2
    try batcher.append(1, 0, "a");
    try batcher.append(1, 1, "b");
    try batcher.append(1, 2, "c");
    // Stream 2: offsets 10, 11
    try batcher.append(2, 10, "x");
    try batcher.append(2, 11, "y");

    const ranges = try batcher.computeStreamRanges();
    defer testing.allocator.free(ranges);

    try testing.expectEqual(@as(usize, 2), ranges.len);

    // Find range for each stream
    var found_s1 = false;
    var found_s2 = false;
    for (ranges) |r| {
        if (r.stream_id == 1) {
            try testing.expectEqual(@as(u64, 0), r.start_offset);
            try testing.expectEqual(@as(u64, 3), r.end_offset); // 2 + 1 record
            found_s1 = true;
        }
        if (r.stream_id == 2) {
            try testing.expectEqual(@as(u64, 10), r.start_offset);
            try testing.expectEqual(@as(u64, 12), r.end_offset); // 11 + 1 record
            found_s2 = true;
        }
    }
    try testing.expect(found_s1);
    try testing.expect(found_s2);
}

test "S3WalBatcher flushNow registers SSO with ObjectManager" {
    const MockS3 = @import("s3.zig").MockS3;

    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();
    batcher.setObjectManager(&om);

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Append data from 2 streams
    try batcher.append(1, 0, "stream1-data");
    try batcher.append(2, 0, "stream2-data");

    // Flush should register a StreamSetObject
    const flushed = batcher.flushNow(&mock_s3);
    try testing.expect(flushed);

    // ObjectManager should have 1 SSO
    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());

    // MockS3 should have 1 object
    try testing.expectEqual(@as(usize, 1), mock_s3.objectCount());

    // Should be queryable for both streams
    const r1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 1), r1.len);

    const r2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 1), r2.len);
}

test "S3WalBatcher setObjectManager" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try testing.expect(batcher.object_manager == null);
    batcher.setObjectManager(&om);
    try testing.expect(batcher.object_manager != null);
}

test "S3WalBatcher trackPendingHW accumulates per-stream max offsets" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    // Track two different streams
    try batcher.trackPendingHW(100, 5);
    try batcher.trackPendingHW(200, 10);
    try testing.expectEqual(@as(u32, 2), batcher.pending_produce_count);

    // Update stream 100 with a higher offset
    try batcher.trackPendingHW(100, 8);
    try testing.expectEqual(@as(u32, 3), batcher.pending_produce_count);

    // Lower offset should NOT replace existing
    try batcher.trackPendingHW(200, 3);
    try testing.expectEqual(@as(u32, 4), batcher.pending_produce_count);

    // Verify values
    try testing.expectEqual(@as(u64, 8), batcher.pending_hw_updates.get(100).?);
    try testing.expectEqual(@as(u64, 10), batcher.pending_hw_updates.get(200).?);
}

test "S3WalBatcher drainPendingHWUpdates returns and resets" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try batcher.trackPendingHW(100, 5);
    try batcher.trackPendingHW(200, 10);
    try testing.expect(batcher.hasPendingFlush());

    // Drain returns the updates
    var updates = batcher.drainPendingHWUpdates();
    defer updates.deinit();

    try testing.expectEqual(@as(u64, 5), updates.get(100).?);
    try testing.expectEqual(@as(u64, 10), updates.get(200).?);

    // Batcher is now reset
    try testing.expect(!batcher.hasPendingFlush());
    try testing.expectEqual(@as(u32, 0), batcher.pending_produce_count);
}

test "S3WalBatcher hasPendingFlush" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try testing.expect(!batcher.hasPendingFlush());

    try batcher.trackPendingHW(1, 0);
    try testing.expect(batcher.hasPendingFlush());

    var updates = batcher.drainPendingHWUpdates();
    defer updates.deinit();
    try testing.expect(!batcher.hasPendingFlush());
}

test "S3WalBatcher last_flushed_segment_id defaults to 0" {
    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();

    try testing.expectEqual(@as(u64, 0), batcher.last_flushed_segment_id);
}

test "S3WalBatcher last_flushed_segment_id tracks after flushNow" {
    const MockS3 = @import("s3.zig").MockS3;

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();
    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Before any flush, last_flushed_segment_id is 0
    try testing.expectEqual(@as(u64, 0), batcher.last_flushed_segment_id);

    // First flush
    try batcher.append(1, 0, "record-a");
    const flushed1 = batcher.flushNow(&mock_s3);
    try testing.expect(flushed1);
    // s3_object_counter was incremented to 1 by flushBuild(), so last_flushed_segment_id = 1
    try testing.expectEqual(@as(u64, 1), batcher.last_flushed_segment_id);

    // Second flush
    try batcher.append(1, 1, "record-b");
    const flushed2 = batcher.flushNow(&mock_s3);
    try testing.expect(flushed2);
    // s3_object_counter was incremented to 2, so last_flushed_segment_id = 2
    try testing.expectEqual(@as(u64, 2), batcher.last_flushed_segment_id);
}

test "S3WalBatcher last_flushed_segment_id unchanged on empty flush" {
    const MockS3 = @import("s3.zig").MockS3;

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();
    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Flush one batch to set last_flushed_segment_id
    try batcher.append(1, 0, "data");
    _ = batcher.flushNow(&mock_s3);
    try testing.expectEqual(@as(u64, 1), batcher.last_flushed_segment_id);

    // Empty flush should not change last_flushed_segment_id
    const flushed = batcher.flushNow(&mock_s3);
    try testing.expect(flushed);
    try testing.expectEqual(@as(u64, 1), batcher.last_flushed_segment_id);
}

test "S3WalBatcher last_flushed_segment_id unchanged when fenced" {
    const MockS3 = @import("s3.zig").MockS3;

    var batcher = S3WalBatcher.init(testing.allocator);
    defer batcher.deinit();
    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    try batcher.append(1, 0, "data");
    batcher.fence();

    // Fenced flush fails, last_flushed_segment_id stays at 0
    const flushed = batcher.flushNow(&mock_s3);
    try testing.expect(!flushed);
    try testing.expectEqual(@as(u64, 0), batcher.last_flushed_segment_id);
}

test "Wal appendSync forces immediate write" {
    const tmp_dir = "/tmp/automq-wal-appendsync-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Write a record using appendSync (which appends + fsyncs)
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();

        const offset = try wal.appendSync("hello-sync");
        try testing.expectEqual(@as(u64, 0), offset);
        try testing.expectEqual(@as(u64, 1), wal.total_records_written);
    }

    // Recover from disk — the record must be present because appendSync
    // guarantees an fsync before returning.
    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();

        const count = try wal.recover(&struct {
            fn cb(_: []const u8, _: u64) void {}
        }.cb);

        try testing.expectEqual(@as(u64, 1), count);
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal sync explicit call succeeds" {
    const tmp_dir = "/tmp/automq-wal-sync-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();

        _ = try wal.append("data-1");
        _ = try wal.append("data-2");
        _ = try wal.append("data-3");

        // Explicit sync should not error
        try wal.sync();

        try testing.expectEqual(@as(u64, 3), wal.total_records_written);
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal cleanupSegments removes old segments" {
    const tmp_dir = "/tmp/automq-wal-cleanup-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        // Use a tiny segment size so each record triggers a rollover.
        // Each record = 9 (header) + data_len bytes.
        // "aaaaaaaaaa" is 10 bytes => 19 bytes per record.
        // With segment_max_size=20, a second record won't fit, forcing a roll.
        const small_segment: usize = 20;
        var wal = Wal.init(testing.allocator, tmp_dir, small_segment);
        defer wal.deinit();
        try wal.open();

        // Append 4 records — each goes into its own segment, creating 3 closed
        // segments plus 1 active segment.
        _ = try wal.append("aaaaaaaaaa"); // seg 0 — fits (19 bytes < 20)... actually next triggers roll
        _ = try wal.append("bbbbbbbbbb"); // rolls seg 0 → closed, writes to seg 1
        _ = try wal.append("cccccccccc"); // rolls seg 1 → closed, writes to seg 2
        _ = try wal.append("dddddddddd"); // rolls seg 2 → closed, writes to seg 3

        const count_before = wal.segmentCount();
        try testing.expect(count_before >= 4);

        // Remove closed segments with id <= 1 (segments 0 and 1)
        const removed = try wal.cleanupSegments(1);
        try testing.expect(removed >= 2);

        const count_after = wal.segmentCount();
        try testing.expect(count_after < count_before);
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal cleanupSegments no-op for empty WAL" {
    const tmp_dir = "/tmp/automq-wal-cleanup-empty-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();

        // No records appended — only the active segment exists, no closed segments.
        // cleanupSegments should safely return 0.
        const removed = try wal.cleanupSegments(0);
        try testing.expectEqual(@as(u64, 0), removed);

        // Segment count should still be 1 (the active segment)
        try testing.expectEqual(@as(usize, 1), wal.segmentCount());
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "Wal segmentCount includes active segment" {
    const tmp_dir = "/tmp/automq-wal-segcount-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var wal = Wal.init(testing.allocator, tmp_dir, 1024 * 1024);
        defer wal.deinit();
        try wal.open();

        // After open, there is exactly 1 segment (the active one)
        try testing.expectEqual(@as(usize, 1), wal.segmentCount());

        // Use a small segment size to force rollover
        wal.segment_max_size = 20;

        // "aaaaaaaaaa" = 10 bytes + 9 header = 19 bytes.
        // First record fits in the active segment.
        _ = try wal.append("aaaaaaaaaa");
        // Second record triggers rollover: old segment becomes closed, new active created.
        _ = try wal.append("bbbbbbbbbb");

        // Should now have >= 2: at least 1 closed + 1 active
        try testing.expect(wal.segmentCount() >= 2);
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

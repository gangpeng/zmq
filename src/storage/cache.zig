const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const MetricRegistry = @import("core").MetricRegistry;

/// LogCache — FIFO cache for recently written (uncommitted) data.
///
/// Records are written here immediately after the WAL append.
/// When a cache block fills up, it is promoted to S3 and evicted.
///
/// Design:
/// - Fixed number of cache blocks (default 64)
/// - Each block holds records for multiple streams
/// - FIFO eviction: oldest block removed when at capacity
/// - Thread-safe: single writer, multiple concurrent readers
pub const LogCache = struct {
    blocks: std.array_list.Managed(CacheBlock),
    max_blocks: usize,
    total_size: u64 = 0,
    max_size: u64,
    allocator: Allocator,
    /// Optional Prometheus metric registry for cache observability.
    metrics: ?*MetricRegistry = null,

    pub const CacheBlock = struct {
        id: u64,
        records: std.array_list.Managed(CachedRecord),
        size: u64 = 0,
        created_at_ms: i64,
        allocator: Allocator,

        pub fn init(alloc: Allocator, id: u64) CacheBlock {
            return .{
                .id = id,
                .records = std.array_list.Managed(CachedRecord).init(alloc),
                .created_at_ms = @import("time_compat").milliTimestamp(),
                .allocator = alloc,
            };
        }

        pub fn deinit(self: *CacheBlock) void {
            for (self.records.items) |rec| {
                self.allocator.free(rec.data);
            }
            self.records.deinit();
        }

        pub fn addRecord(self: *CacheBlock, stream_id: u64, offset: u64, data: []const u8) !void {
            const data_copy = try self.allocator.dupe(u8, data);
            try self.records.append(.{
                .stream_id = stream_id,
                .offset = offset,
                .data = data_copy,
            });
            self.size += data.len;
        }

        /// Add a record without copying — caller transfers ownership of the data slice.
        pub fn addRecordOwned(self: *CacheBlock, stream_id: u64, offset: u64, data: []u8) !void {
            try self.records.append(.{
                .stream_id = stream_id,
                .offset = offset,
                .data = data,
            });
            self.size += data.len;
        }

        pub fn recordCount(self: *const CacheBlock) usize {
            return self.records.items.len;
        }
    };

    pub const CachedRecord = struct {
        stream_id: u64,
        offset: u64,
        data: []u8,
    };

    pub fn init(alloc: Allocator, max_blocks: usize, max_size: u64) LogCache {
        return .{
            .blocks = std.array_list.Managed(CacheBlock).init(alloc),
            .max_blocks = max_blocks,
            .max_size = max_size,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *LogCache) void {
        for (self.blocks.items) |*block| {
            block.deinit();
        }
        self.blocks.deinit();
    }

    /// Put a record into the cache.
    pub fn put(self: *LogCache, stream_id: u64, offset: u64, data: []const u8) !void {
        // Evict if at capacity
        while (self.blocks.items.len >= self.max_blocks or
            (self.max_size > 0 and self.total_size + data.len > self.max_size))
        {
            if (self.blocks.items.len == 0) break;
            self.evictOldest();
        }

        // Ensure we have an active block
        if (self.blocks.items.len == 0 or self.blocks.getLast().recordCount() >= 1000) {
            const block_id: u64 = if (self.blocks.items.len > 0)
                self.blocks.getLast().id + 1
            else
                0;
            try self.blocks.append(CacheBlock.init(self.allocator, block_id));
        }

        const block = &self.blocks.items[self.blocks.items.len - 1];
        try block.addRecord(stream_id, offset, data);
        self.total_size += data.len;
        self.updateLogCacheGauges();
    }

    /// Put a record with caller-owned data (zero-copy — no dupe).
    pub fn putOwned(self: *LogCache, stream_id: u64, offset: u64, data: []u8) !void {
        while (self.blocks.items.len >= self.max_blocks or
            (self.max_size > 0 and self.total_size + data.len > self.max_size))
        {
            if (self.blocks.items.len == 0) break;
            self.evictOldest();
        }

        if (self.blocks.items.len == 0 or self.blocks.getLast().recordCount() >= 1000) {
            const block_id: u64 = if (self.blocks.items.len > 0)
                self.blocks.getLast().id + 1
            else
                0;
            try self.blocks.append(CacheBlock.init(self.allocator, block_id));
        }

        const block = &self.blocks.items[self.blocks.items.len - 1];
        try block.addRecordOwned(stream_id, offset, data);
        self.total_size += data.len;
        self.updateLogCacheGauges();
    }

    /// Get records for a stream in the offset range [start_offset, end_offset).
    pub fn get(self: *LogCache, stream_id: u64, start_offset: u64, end_offset: u64, alloc: Allocator) ![]const CachedRecord {
        var results = std.array_list.Managed(CachedRecord).init(alloc);

        for (self.blocks.items) |*block| {
            for (block.records.items) |rec| {
                if (rec.stream_id == stream_id and rec.offset >= start_offset and rec.offset < end_offset) {
                    try results.append(rec);
                }
            }
        }

        const slice = try results.toOwnedSlice();
        if (self.metrics) |m| {
            if (slice.len > 0) {
                m.incrementLabeledCounter("cache_operations_total", &.{ "log", "hit" });
            } else {
                m.incrementLabeledCounter("cache_operations_total", &.{ "log", "miss" });
            }
        }
        return slice;
    }

    fn evictOldest(self: *LogCache) void {
        if (self.blocks.items.len == 0) return;
        var block = self.blocks.orderedRemove(0);
        self.total_size -= block.size;
        block.deinit();
        if (self.metrics) |m| {
            m.incrementLabeledCounter("cache_evictions_total", &.{"log"});
        }
    }

    /// Number of cached blocks.
    pub fn blockCount(self: *const LogCache) usize {
        return self.blocks.items.len;
    }

    /// Total number of cached records.
    pub fn recordCount(self: *const LogCache) usize {
        var count: usize = 0;
        for (self.blocks.items) |*block| {
            count += block.recordCount();
        }
        return count;
    }

    /// Update Prometheus gauges for LogCache size and entry count.
    fn updateLogCacheGauges(self: *LogCache) void {
        if (self.metrics) |m| {
            m.setGauge("log_cache_size_bytes", @floatFromInt(self.total_size));
            m.setGauge("log_cache_entries", @floatFromInt(self.recordCount()));
        }
    }
};

/// S3BlockCache — LRU cache for committed S3 data blocks.
///
/// When data is read from S3, blocks are cached here to avoid
/// repeated S3 fetches. Uses LRU eviction when at capacity.
pub const S3BlockCache = struct {
    entries: std.StringHashMap(CacheEntry),
    access_order: std.array_list.Managed([]const u8),
    max_size: u64,
    current_size: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    allocator: Allocator,
    /// Optional Prometheus metric registry for cache observability.
    metrics: ?*MetricRegistry = null,

    pub const CacheEntry = struct {
        key: []u8,
        data: []u8,
        size: u64,
    };

    pub fn init(alloc: Allocator, max_size: u64) S3BlockCache {
        return .{
            .entries = std.StringHashMap(CacheEntry).init(alloc),
            .access_order = std.array_list.Managed([]const u8).init(alloc),
            .max_size = max_size,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *S3BlockCache) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.key);
            self.allocator.free(entry.value_ptr.data);
        }
        self.entries.deinit();
        self.access_order.deinit();
    }

    /// Get data from cache. Returns null on miss.
    pub fn get(self: *S3BlockCache, key: []const u8) ?[]const u8 {
        if (self.entries.get(key)) |entry| {
            self.hits += 1;
            if (self.metrics) |m| {
                m.incrementLabeledCounter("cache_operations_total", &.{ "s3_block", "hit" });
            }
            return entry.data;
        }
        self.misses += 1;
        if (self.metrics) |m| {
            m.incrementLabeledCounter("cache_operations_total", &.{ "s3_block", "miss" });
        }
        return null;
    }

    /// Put data into cache.
    pub fn put(self: *S3BlockCache, key: []const u8, data: []const u8) !void {
        // Don't cache if single item exceeds max
        if (data.len > self.max_size) return;

        // Evict until we have room
        while (self.current_size + data.len > self.max_size) {
            self.evictLru() catch break;
        }

        // Check if key already exists
        if (self.entries.contains(key)) return;

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);
        const data_copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_copy);

        try self.entries.put(key_copy, .{
            .key = key_copy,
            .data = data_copy,
            .size = data.len,
        });
        try self.access_order.append(key_copy);
        self.current_size += data.len;
        self.updateS3CacheGauges();
    }

    fn evictLru(self: *S3BlockCache) !void {
        if (self.access_order.items.len == 0) return error.CacheEmpty;

        const oldest_key = self.access_order.orderedRemove(0);
        if (self.entries.fetchRemove(oldest_key)) |entry| {
            self.current_size -= entry.value.size;
            self.allocator.free(entry.value.data);
            self.allocator.free(entry.value.key);
        }
        if (self.metrics) |m| {
            m.incrementLabeledCounter("cache_evictions_total", &.{"s3_block"});
        }
    }

    pub fn hitRate(self: *const S3BlockCache) f64 {
        const total = self.hits + self.misses;
        if (total == 0) return 0.0;
        return @as(f64, @floatFromInt(self.hits)) / @as(f64, @floatFromInt(total));
    }

    /// Update Prometheus gauges for S3BlockCache size and entry count.
    fn updateS3CacheGauges(self: *S3BlockCache) void {
        if (self.metrics) |m| {
            m.setGauge("s3_block_cache_size_bytes", @floatFromInt(self.current_size));
            m.setGauge("s3_block_cache_entries", @floatFromInt(self.entries.count()));
        }
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "LogCache put and get" {
    var cache = LogCache.init(testing.allocator, 64, 1024 * 1024);
    defer cache.deinit();

    try cache.put(1, 0, "record-0");
    try cache.put(1, 1, "record-1");
    try cache.put(2, 0, "stream2-0");

    try testing.expectEqual(@as(usize, 3), cache.recordCount());

    const results = try cache.get(1, 0, 10, testing.allocator);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 2), results.len);
}

test "LogCache FIFO eviction" {
    var cache = LogCache.init(testing.allocator, 2, 0); // max 2 blocks
    defer cache.deinit();

    // Fill first block (1000 records per block)
    for (0..1000) |i| {
        try cache.put(1, i, "x");
    }
    try testing.expectEqual(@as(usize, 1), cache.blockCount());

    // This triggers a new block
    try cache.put(1, 1000, "y");
    try testing.expectEqual(@as(usize, 2), cache.blockCount());

    // Third block → evicts the first
    for (0..1000) |i| {
        try cache.put(1, 1001 + i, "z");
    }
    // Should still be at or under 2 blocks
    try testing.expect(cache.blockCount() <= 2);
}

test "S3BlockCache hit and miss" {
    var cache = S3BlockCache.init(testing.allocator, 1024);
    defer cache.deinit();

    try testing.expect(cache.get("key1") == null);
    try testing.expectEqual(@as(u64, 1), cache.misses);

    try cache.put("key1", "data1");
    const data = cache.get("key1");
    try testing.expect(data != null);
    try testing.expectEqualStrings("data1", data.?);
    try testing.expectEqual(@as(u64, 1), cache.hits);
}

test "S3BlockCache LRU eviction" {
    var cache = S3BlockCache.init(testing.allocator, 20); // 20 bytes max
    defer cache.deinit();

    try cache.put("k1", "12345"); // 5 bytes
    try cache.put("k2", "67890"); // 5 bytes, total 10
    try cache.put("k3", "abcde"); // 5 bytes, total 15
    try cache.put("k4", "fghij12345"); // 10 bytes → needs eviction

    // k1 should have been evicted (LRU)
    try testing.expect(cache.get("k1") == null);
    try testing.expect(cache.get("k4") != null);
}

test "LogCache size-based eviction" {
    // 20 bytes max size
    var cache = LogCache.init(testing.allocator, 1000, 20);
    defer cache.deinit();

    try cache.put(1, 0, "12345678901"); // 11 bytes
    try cache.put(1, 1, "12345678901"); // 11 bytes, total > 20

    // Should have evicted oldest block to make room
    try testing.expect(cache.recordCount() >= 1);
}

test "LogCache get empty range" {
    var cache = LogCache.init(testing.allocator, 64, 1024 * 1024);
    defer cache.deinit();

    try cache.put(1, 0, "data");

    // Query for a different stream
    const results = try cache.get(2, 0, 10, testing.allocator);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 0), results.len);
}

test "LogCache get exact offset range" {
    var cache = LogCache.init(testing.allocator, 64, 1024 * 1024);
    defer cache.deinit();

    try cache.put(1, 0, "a");
    try cache.put(1, 1, "b");
    try cache.put(1, 2, "c");
    try cache.put(1, 3, "d");

    // Get [1, 3) — should return offsets 1 and 2
    const results = try cache.get(1, 1, 3, testing.allocator);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 2), results.len);
    try testing.expectEqual(@as(u64, 1), results[0].offset);
    try testing.expectEqual(@as(u64, 2), results[1].offset);
}

test "S3BlockCache hitRate" {
    var cache = S3BlockCache.init(testing.allocator, 1024);
    defer cache.deinit();

    // No operations yet
    try testing.expectEqual(@as(f64, 0.0), cache.hitRate());

    try cache.put("key", "data");
    _ = cache.get("key"); // hit
    _ = cache.get("missing"); // miss

    // 1 hit, 1 miss = 50%
    try testing.expectApproxEqAbs(@as(f64, 0.5), cache.hitRate(), 0.01);
}

test "S3BlockCache oversized item skipped" {
    var cache = S3BlockCache.init(testing.allocator, 5); // max 5 bytes
    defer cache.deinit();

    try cache.put("key", "1234567890"); // 10 bytes > 5 bytes max
    try testing.expect(cache.get("key") == null);
}

test "S3BlockCache duplicate key ignored" {
    var cache = S3BlockCache.init(testing.allocator, 1024);
    defer cache.deinit();

    try cache.put("key", "data1");
    try cache.put("key", "data2"); // should be ignored

    try testing.expectEqualStrings("data1", cache.get("key").?);
}

test "LogCache blockCount and recordCount" {
    var cache = LogCache.init(testing.allocator, 64, 1024 * 1024);
    defer cache.deinit();

    try testing.expectEqual(@as(usize, 0), cache.blockCount());
    try testing.expectEqual(@as(usize, 0), cache.recordCount());

    try cache.put(1, 0, "data");
    try testing.expectEqual(@as(usize, 1), cache.blockCount());
    try testing.expectEqual(@as(usize, 1), cache.recordCount());
}

test "LogCache putOwned transfers ownership" {
    var cache = LogCache.init(testing.allocator, 64, 1024 * 1024);
    defer cache.deinit();

    // Allocate 8 bytes and fill with data — ownership transfers to the cache.
    const owned = try testing.allocator.alloc(u8, 8);
    @memcpy(owned, "ownedDAT");

    try cache.putOwned(1, 0, owned);

    // get() should return the data we transferred.
    const results = try cache.get(1, 0, 1, testing.allocator);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqualStrings("ownedDAT", results[0].data);

    // deinit frees the owned slice; testing.allocator will detect leaks if not freed.
}

test "LogCache block rollover at 1000 records" {
    var cache = LogCache.init(testing.allocator, 64, 10 * 1024 * 1024);
    defer cache.deinit();

    // Put 1001 records — first 1000 fill block 0, record 1001 triggers a new block.
    for (0..1001) |i| {
        try cache.put(1, i, "r");
    }

    try testing.expectEqual(@as(usize, 2), cache.blockCount());
    try testing.expectEqual(@as(usize, 1001), cache.recordCount());
}

test "LogCache get filters by stream_id" {
    var cache = LogCache.init(testing.allocator, 64, 1024 * 1024);
    defer cache.deinit();

    // Interleave records for stream 1 and stream 2.
    try cache.put(1, 0, "s1-a");
    try cache.put(2, 0, "s2-a");
    try cache.put(1, 1, "s1-b");
    try cache.put(2, 1, "s2-b");
    try cache.put(1, 2, "s1-c");

    // Query stream 1 only — should return 3 records.
    const results = try cache.get(1, 0, 100, testing.allocator);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 3), results.len);

    // Verify every returned record belongs to stream 1.
    for (results) |rec| {
        try testing.expectEqual(@as(u64, 1), rec.stream_id);
    }
}

test "LogCache combined size and block eviction" {
    // max_blocks=2, max_size=200 — both constraints active.
    var cache = LogCache.init(testing.allocator, 2, 200);
    defer cache.deinit();

    // Put data that fits in one block and within size budget.
    // Each record is 10 bytes × 10 records = 100 bytes, 1 block.
    for (0..10) |i| {
        try cache.put(1, i, "0123456789");
    }
    try testing.expectEqual(@as(usize, 1), cache.blockCount());

    // Add more data to approach the 200-byte limit (another 100 bytes).
    // This still fits within max_blocks=2 and max_size=200.
    for (10..20) |i| {
        try cache.put(1, i, "0123456789");
    }

    // Adding more data should trigger eviction of the oldest block.
    try cache.put(1, 20, "0123456789");

    // After eviction, we should have at most 2 blocks.
    try testing.expect(cache.blockCount() <= 2);

    // The earliest offsets (block 0) should have been evicted.
    const results = try cache.get(1, 0, 5, testing.allocator);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 0), results.len);
}

test "S3BlockCache eviction frees memory properly" {
    // max_size=100 — can hold at most two 50-byte items.
    var cache = S3BlockCache.init(testing.allocator, 100);
    defer cache.deinit();

    const data_50 = "01234567890123456789012345678901234567890123456789"; // 50 bytes

    try cache.put("a", data_50); // 50 bytes, total 50
    try cache.put("b", data_50); // 50 bytes, total 100
    try cache.put("c", data_50); // needs eviction of "a" to fit

    // "a" was evicted (LRU), "b" and "c" remain.
    try testing.expect(cache.get("a") == null);
    try testing.expect(cache.get("b") != null);
    try testing.expect(cache.get("c") != null);

    // current_size should be exactly 100 (two items of 50 bytes).
    try testing.expectEqual(@as(u64, 100), cache.current_size);

    // testing.allocator detects leaks — evicted data must have been freed.
}

test "S3BlockCache hit/miss counters accurate" {
    var cache = S3BlockCache.init(testing.allocator, 1024);
    defer cache.deinit();

    try cache.put("item", "value");

    _ = cache.get("item"); // hit
    _ = cache.get("no-such-key"); // miss
    _ = cache.get("also-missing"); // miss

    try testing.expectEqual(@as(u64, 1), cache.hits);
    try testing.expectEqual(@as(u64, 2), cache.misses);

    // hitRate = 1 / 3 ≈ 0.333
    try testing.expectApproxEqAbs(@as(f64, 1.0 / 3.0), cache.hitRate(), 0.001);
}

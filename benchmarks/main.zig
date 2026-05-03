const std = @import("std");
const broker = @import("broker");
const protocol = @import("protocol");
const storage = @import("storage");
const time = @import("time_compat");

/// AutoMQ Performance Benchmarks
///
/// Run with: zig build bench
pub fn main() !void {
    const stdout = BenchmarkWriter{};

    try stdout.print("\n=== AutoMQ Zig Benchmarks ===\n\n", .{});

    try benchVarint(stdout);
    try benchCrc32c(stdout);
    try benchByteBuffer(stdout);
    try benchPartitionStoreMemory(stdout);
    try benchPartitionStoreS3Wal(stdout);

    try stdout.print("\n=== Benchmarks complete ===\n", .{});
}

const BenchmarkWriter = struct {
    pub fn print(_: BenchmarkWriter, comptime fmt: []const u8, args: anytype) !void {
        std.debug.print(fmt, args);
    }
};

fn benchVarint(writer: anytype) !void {
    const core = @import("core");
    const varint = core.Varint;

    var buf: [10]u8 = undefined;
    const iterations: usize = 10_000_000;

    // Benchmark unsigned varint32 encode
    var start_ns = nowNs();

    var encoded_total: u64 = 0;
    for (0..iterations) |i| {
        encoded_total += varint.encodeUnsignedVarint32(&buf, @truncate(i));
    }
    std.mem.doNotOptimizeAway(encoded_total);

    const encode_ns = nowNs() - start_ns;
    const encode_rate = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(encode_ns)) / 1e9);

    try writer.print("Varint32 encode:   {d:.1} M/sec ({d:.2} ns/op)\n", .{
        encode_rate / 1e6,
        @as(f64, @floatFromInt(encode_ns)) / @as(f64, @floatFromInt(iterations)),
    });

    // Benchmark unsigned varint32 decode
    _ = varint.encodeUnsignedVarint32(&buf, 300); // 2-byte varint
    start_ns = nowNs();

    for (0..iterations) |_| {
        const result = varint.decodeUnsignedVarint32(&buf) catch unreachable;
        std.mem.doNotOptimizeAway(result);
    }

    const decode_ns = nowNs() - start_ns;
    const decode_rate = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(decode_ns)) / 1e9);

    try writer.print("Varint32 decode:   {d:.1} M/sec ({d:.2} ns/op)\n", .{
        decode_rate / 1e6,
        @as(f64, @floatFromInt(decode_ns)) / @as(f64, @floatFromInt(iterations)),
    });
}

fn benchCrc32c(writer: anytype) !void {
    const core = @import("core");
    const crc32c = core.Crc32c;

    // 4KB data block (typical Kafka record batch size)
    var data: [4096]u8 = undefined;
    for (&data, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    const iterations: usize = 1_000_000;
    const start_ns = nowNs();

    for (0..iterations) |_| {
        const crc = crc32c.compute(&data);
        std.mem.doNotOptimizeAway(crc);
    }

    const ns = nowNs() - start_ns;
    const bytes_total = @as(f64, @floatFromInt(iterations)) * 4096.0;
    const throughput_gbps = bytes_total / (@as(f64, @floatFromInt(ns)) / 1e9) / 1e9;

    try writer.print("CRC-32C (4KB):     {d:.2} GB/s ({d:.2} ns/op)\n", .{
        throughput_gbps,
        @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(iterations)),
    });
}

fn benchByteBuffer(writer: anytype) !void {
    const core = @import("core");
    const ByteBuffer = core.ByteBuffer;

    var buf_data: [1024]u8 = undefined;
    const iterations: usize = 10_000_000;

    const start_ns = nowNs();

    for (0..iterations) |_| {
        var buf = ByteBuffer.wrap(&buf_data);
        buf.writeI32(42) catch unreachable;
        buf.writeI64(123456789) catch unreachable;
        buf.writeI16(-1) catch unreachable;
        buf.flip();
        const a = buf.readI32() catch unreachable;
        const b = buf.readI64() catch unreachable;
        const c = buf.readI16() catch unreachable;
        std.mem.doNotOptimizeAway(a);
        std.mem.doNotOptimizeAway(b);
        std.mem.doNotOptimizeAway(c);
    }

    const ns = nowNs() - start_ns;
    const rate = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(ns)) / 1e9);

    try writer.print("ByteBuffer R/W:    {d:.1} M/sec ({d:.2} ns/op)\n", .{
        rate / 1e6,
        @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(iterations)),
    });
}

const TimedResult = struct {
    throughput: f64,
    p50_ns: u64,
    p99_ns: u64,
    elapsed_ns: u64,
};

fn nowNs() u64 {
    return @intCast(time.nanoTimestamp());
}

fn lessThanU64(_: void, a: u64, b: u64) bool {
    return a < b;
}

fn percentile(sorted: []const u64, pct: u64) u64 {
    if (sorted.len == 0) return 0;
    const rank = @max(@as(usize, 1), (sorted.len * @as(usize, @intCast(pct)) + 99) / 100);
    return sorted[@min(sorted.len - 1, rank - 1)];
}

fn summarize(latencies: []u64, elapsed_ns: u64) TimedResult {
    std.mem.sort(u64, latencies, {}, lessThanU64);
    const elapsed_secs = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    return .{
        .throughput = @as(f64, @floatFromInt(latencies.len)) / elapsed_secs,
        .p50_ns = percentile(latencies, 50),
        .p99_ns = percentile(latencies, 99),
        .elapsed_ns = elapsed_ns,
    };
}

fn printTimedResult(writer: anytype, name: []const u8, result: TimedResult) !void {
    try writer.print("{s:<24} {d:>10.0}/s  p50={d:>8.2} us  p99={d:>8.2} us  total={d:.2} ms\n", .{
        name,
        result.throughput,
        @as(f64, @floatFromInt(result.p50_ns)) / 1e3,
        @as(f64, @floatFromInt(result.p99_ns)) / 1e3,
        @as(f64, @floatFromInt(result.elapsed_ns)) / 1e6,
    });
}

fn requirePerformance(name: []const u8, result: TimedResult, min_ops_per_sec: f64, max_p99_ms: f64) !void {
    const p99_ms = @as(f64, @floatFromInt(result.p99_ns)) / 1e6;
    if (result.throughput < min_ops_per_sec or p99_ms > max_p99_ms) {
        std.debug.print(
            "benchmark gate failed for {s}: throughput={d:.2}/s min={d:.2}/s p99={d:.2}ms max={d:.2}ms\n",
            .{ name, result.throughput, min_ops_per_sec, p99_ms, max_p99_ms },
        );
        return error.BenchmarkGateFailed;
    }
}

fn buildBenchmarkBatch(alloc: std.mem.Allocator, payload_len: usize) ![]u8 {
    const payload = try alloc.alloc(u8, payload_len);
    defer alloc.free(payload);
    for (payload, 0..) |*byte, i| {
        byte.* = @truncate(i);
    }

    const records = [_]protocol.Record{.{
        .offset_delta = 0,
        .value = payload,
    }};

    return try protocol.RecordBatch.buildRecordBatch(
        alloc,
        0,
        &records,
        -1,
        -1,
        -1,
        0,
        0,
        0,
    );
}

fn benchPartitionStoreMemory(writer: anytype) !void {
    const alloc = std.heap.page_allocator;
    const topic = "bench-partition-store";
    const partition: i32 = 0;
    const warmup: usize = 1_000;
    const produce_iterations: usize = 20_000;
    const fetch_iterations: usize = 5_000;

    var store = broker.PartitionStore.initWithConfig(alloc, .{
        .cache_max_blocks = 128,
        .cache_max_size = 64 * 1024 * 1024,
    });
    defer store.deinit();

    const batch = try buildBenchmarkBatch(alloc, 256);
    defer alloc.free(batch);

    for (0..warmup) |_| {
        _ = try store.produce(topic, partition, batch);
    }

    const produce_latencies = try alloc.alloc(u64, produce_iterations);
    defer alloc.free(produce_latencies);

    const produce_start = nowNs();
    for (produce_latencies) |*latency| {
        const start = nowNs();
        _ = try store.produce(topic, partition, batch);
        latency.* = nowNs() - start;
    }
    const produce_result = summarize(produce_latencies, nowNs() - produce_start);
    try printTimedResult(writer, "PartitionStore produce", produce_result);
    try requirePerformance("PartitionStore produce", produce_result, 500.0, 250.0);

    const produced_records = warmup + produce_iterations;
    for (0..warmup) |i| {
        const fetched = try store.fetch(topic, partition, @intCast(i % produced_records), 64 * 1024);
        if (fetched.error_code != 0) return error.BenchmarkFetchFailed;
        if (fetched.records.len > 0) alloc.free(@constCast(fetched.records));
    }

    const fetch_latencies = try alloc.alloc(u64, fetch_iterations);
    defer alloc.free(fetch_latencies);

    const fetch_start = nowNs();
    for (fetch_latencies, 0..) |*latency, i| {
        const start = nowNs();
        const fetched = try store.fetch(topic, partition, @intCast(i % produced_records), 64 * 1024);
        if (fetched.error_code != 0) return error.BenchmarkFetchFailed;
        if (fetched.records.len == 0) return error.BenchmarkFetchEmpty;
        alloc.free(@constCast(fetched.records));
        latency.* = nowNs() - start;
    }
    const fetch_result = summarize(fetch_latencies, nowNs() - fetch_start);
    try printTimedResult(writer, "PartitionStore fetch", fetch_result);
    try requirePerformance("PartitionStore fetch", fetch_result, 100.0, 500.0);
}

fn benchPartitionStoreS3Wal(writer: anytype) !void {
    const alloc = std.heap.page_allocator;
    const topic = "bench-s3-wal";
    const partition: i32 = 0;
    const warmup: usize = 10;
    const iterations: usize = 200;

    var mock_s3 = storage.MockS3.init(alloc);
    defer mock_s3.deinit();
    const s3_storage = storage.S3Storage.initMock(alloc, &mock_s3);

    var store = broker.PartitionStore.initWithConfig(alloc, .{
        .cache_max_blocks = 256,
        .cache_max_size = 128 * 1024 * 1024,
    });
    defer store.deinit();
    store.s3_wal_mode = true;
    store.s3_storage = s3_storage;
    store.s3_wal_batcher = storage.wal.S3WalBatcher.initWithConfig(alloc, .{
        .batch_size = 4 * 1024 * 1024,
        .flush_interval_ms = 250,
        .flush_mode = .sync,
    });

    const batch = try buildBenchmarkBatch(alloc, 4 * 1024);
    defer alloc.free(batch);

    for (0..warmup) |_| {
        _ = try store.produce(topic, partition, batch);
    }

    const before_puts = mock_s3.put_count;
    const before_lists = mock_s3.list_count;
    const latencies = try alloc.alloc(u64, iterations);
    defer alloc.free(latencies);

    const start_all = nowNs();
    for (latencies) |*latency| {
        const start = nowNs();
        _ = try store.produce(topic, partition, batch);
        latency.* = nowNs() - start;
    }
    const result = summarize(latencies, nowNs() - start_all);
    try printTimedResult(writer, "S3 WAL sync produce", result);
    try requirePerformance("S3 WAL sync produce", result, 5.0, 1_000.0);

    const measured_puts = mock_s3.put_count - before_puts;
    const measured_lists = mock_s3.list_count - before_lists;
    const measured_bytes = @as(f64, @floatFromInt(batch.len * iterations));
    const mib = measured_bytes / (1024.0 * 1024.0);
    const requests_per_mib = @as(f64, @floatFromInt(measured_puts + measured_lists)) / mib;
    try writer.print("S3 WAL request volume    puts={d} lists={d} requests/MiB={d:.2}\n", .{
        measured_puts,
        measured_lists,
        requests_per_mib,
    });

    var replacement_om = storage.ObjectManager.init(alloc, 1);
    defer replacement_om.deinit();
    var replacement = broker.PartitionStore.init(alloc);
    defer replacement.deinit();
    replacement.s3_storage = s3_storage;
    replacement.object_manager = &replacement_om;

    const rebuild_start = nowNs();
    const recovered = try replacement.recoverS3WalObjects();
    try replacement.ensurePartition(topic, partition);
    if (!replacement.repairPartitionStatesFromObjectManager()) return error.BenchmarkRecoveryFailed;
    const rebuild_ns = nowNs() - rebuild_start;

    const fetched = try replacement.fetch(topic, partition, 0, 1024 * 1024);
    defer if (fetched.records.len > 0) alloc.free(@constCast(fetched.records));
    if (fetched.error_code != 0 or fetched.records.len == 0) return error.BenchmarkRecoveryFetchFailed;

    const recovered_per_sec = @as(f64, @floatFromInt(recovered)) / (@as(f64, @floatFromInt(rebuild_ns)) / 1e9);
    try writer.print("S3 WAL rebuild           objects={d} rate={d:.0}/s total={d:.2} ms\n", .{
        recovered,
        recovered_per_sec,
        @as(f64, @floatFromInt(rebuild_ns)) / 1e6,
    });
}

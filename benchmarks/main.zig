const std = @import("std");

/// AutoMQ Performance Benchmarks
///
/// Run with: zig build bench
pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("\n=== AutoMQ Zig Benchmarks ===\n\n", .{});

    try benchVarint(stdout);
    try benchCrc32c(stdout);
    try benchByteBuffer(stdout);

    try stdout.print("\n=== Benchmarks complete ===\n", .{});
}

fn benchVarint(writer: anytype) !void {
    const core = @import("core");
    const varint = core.Varint;

    var buf: [10]u8 = undefined;
    const iterations: usize = 10_000_000;

    // Benchmark unsigned varint32 encode
    var timer = std.time.Timer.start() catch unreachable;

    for (0..iterations) |i| {
        _ = varint.encodeUnsignedVarint32(&buf, @truncate(i));
    }

    const encode_ns = timer.read();
    const encode_rate = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(encode_ns)) / 1e9);

    try writer.print("Varint32 encode:   {d:.1} M/sec ({d} ns/op)\n", .{
        encode_rate / 1e6,
        encode_ns / iterations,
    });

    // Benchmark unsigned varint32 decode
    _ = varint.encodeUnsignedVarint32(&buf, 300); // 2-byte varint
    timer.reset();

    for (0..iterations) |_| {
        const result = varint.decodeUnsignedVarint32(&buf) catch unreachable;
        std.mem.doNotOptimizeAway(result);
    }

    const decode_ns = timer.read();
    const decode_rate = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(decode_ns)) / 1e9);

    try writer.print("Varint32 decode:   {d:.1} M/sec ({d} ns/op)\n", .{
        decode_rate / 1e6,
        decode_ns / iterations,
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
    var timer = std.time.Timer.start() catch unreachable;

    for (0..iterations) |_| {
        const crc = crc32c.compute(&data);
        std.mem.doNotOptimizeAway(crc);
    }

    const ns = timer.read();
    const bytes_total = @as(f64, @floatFromInt(iterations)) * 4096.0;
    const throughput_gbps = bytes_total / (@as(f64, @floatFromInt(ns)) / 1e9) / 1e9;

    try writer.print("CRC-32C (4KB):     {d:.2} GB/s ({d} ns/op)\n", .{
        throughput_gbps,
        ns / iterations,
    });
}

fn benchByteBuffer(writer: anytype) !void {
    const core = @import("core");
    const ByteBuffer = core.ByteBuffer;

    var buf_data: [1024]u8 = undefined;
    const iterations: usize = 10_000_000;

    var timer = std.time.Timer.start() catch unreachable;

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

    const ns = timer.read();
    const rate = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(ns)) / 1e9);

    try writer.print("ByteBuffer R/W:    {d:.1} M/sec ({d} ns/op)\n", .{
        rate / 1e6,
        ns / iterations,
    });
}

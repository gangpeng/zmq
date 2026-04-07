const std = @import("std");
const testing = std.testing;

/// CRC-32C (Castagnoli) checksum implementation.
///
/// Used by Kafka for record batch integrity validation. Kafka uses CRC-32C
/// (polynomial 0x1EDC6F41) rather than the more common CRC-32 (Ethernet).
///
/// This implementation uses a software lookup table. On x86_64 with SSE4.2,
/// hardware acceleration can be added via inline assembly for production
/// throughput targets (>10 GB/s).

const CRC32C_POLY = 0x82F63B78; // Reversed polynomial for CRC-32C

/// Pre-computed CRC-32C lookup table (256 entries).
const crc_table: [256]u32 = blk: {
    @setEvalBranchQuota(10000);
    var table: [256]u32 = undefined;
    for (0..256) |i| {
        var crc: u32 = @intCast(i);
        for (0..8) |_| {
            if (crc & 1 != 0) {
                crc = (crc >> 1) ^ CRC32C_POLY;
            } else {
                crc = crc >> 1;
            }
        }
        table[i] = crc;
    }
    break :blk table;
};

/// Compute CRC-32C of the given data.
pub fn compute(data: []const u8) u32 {
    return update(0xFFFFFFFF, data) ^ 0xFFFFFFFF;
}

/// Update a running CRC-32C with additional data.
/// Initial value should be 0xFFFFFFFF; finalize with XOR 0xFFFFFFFF.
pub fn update(crc: u32, data: []const u8) u32 {
    var c = crc;
    for (data) |byte| {
        c = crc_table[@as(u8, @truncate(c)) ^ byte] ^ (c >> 8);
    }
    return c;
}

/// Compute CRC-32C, matching the Kafka convention:
/// Kafka computes CRC over record batch data and stores it as a u32.
pub fn kafkaCrc(data: []const u8) u32 {
    return compute(data);
}

/// Verify a CRC-32C checksum matches the expected value.
pub fn verify(data: []const u8, expected: u32) bool {
    return compute(data) == expected;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "CRC-32C empty" {
    try testing.expectEqual(@as(u32, 0x00000000), compute(""));
}

test "CRC-32C known vectors" {
    // Standard CRC-32C test vectors
    // "123456789" → 0xE3069283
    try testing.expectEqual(@as(u32, 0xE3069283), compute("123456789"));
}

test "CRC-32C incremental" {
    const data = "Hello, Kafka!";

    // Compute in one shot
    const one_shot = compute(data);

    // Compute incrementally
    var crc = update(0xFFFFFFFF, data[0..5]);
    crc = update(crc, data[5..]);
    const incremental = crc ^ 0xFFFFFFFF;

    try testing.expectEqual(one_shot, incremental);
}

test "CRC-32C verify" {
    const data = "test data for crc";
    const crc = compute(data);
    try testing.expect(verify(data, crc));
    try testing.expect(!verify(data, crc ^ 1)); // Flipped bit should fail
}

test "CRC-32C all zeros" {
    var zeros: [32]u8 = [_]u8{0} ** 32;
    const crc = compute(&zeros);
    // Just verify it's deterministic and non-zero
    try testing.expect(crc != 0);
    try testing.expectEqual(crc, compute(&zeros));
}

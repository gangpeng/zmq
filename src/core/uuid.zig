const std = @import("std");
const testing = std.testing;
const crypto = std.crypto;

/// 128-bit UUID as used by Kafka (e.g., topic IDs, producer IDs).
///
/// Stored as 16 bytes in big-endian order, matching Kafka's wire format
/// where UUIDs are transmitted as 16 raw bytes.
pub const Uuid = struct {
    bytes: [16]u8,

    pub const ZERO: Uuid = .{ .bytes = [_]u8{0} ** 16 };

    /// Generate a random (v4) UUID.
    pub fn random() Uuid {
        var bytes: [16]u8 = undefined;
        crypto.random.bytes(&bytes);

        // Set version 4 (random)
        bytes[6] = (bytes[6] & 0x0F) | 0x40;
        // Set variant 1 (RFC 4122)
        bytes[8] = (bytes[8] & 0x3F) | 0x80;

        return .{ .bytes = bytes };
    }

    /// Create a UUID from raw bytes.
    pub fn fromBytes(bytes: [16]u8) Uuid {
        return .{ .bytes = bytes };
    }

    /// Parse a UUID from its string representation.
    /// Format: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" (36 chars)
    pub fn fromString(str: []const u8) !Uuid {
        return fromStringSimple(str);
    }

    fn fromStringSimple(str: []const u8) !Uuid {
        // Remove dashes and decode hex
        var hex_chars: [32]u8 = undefined;
        var hex_idx: usize = 0;
        for (str) |c| {
            if (c == '-') continue;
            if (hex_idx >= 32) return error.InvalidUuidFormat;
            hex_chars[hex_idx] = c;
            hex_idx += 1;
        }
        if (hex_idx != 32) return error.InvalidUuidFormat;

        var bytes: [16]u8 = undefined;
        for (0..16) |i| {
            const high = hexDigit(hex_chars[i * 2]) orelse return error.InvalidUuidFormat;
            const low = hexDigit(hex_chars[i * 2 + 1]) orelse return error.InvalidUuidFormat;
            bytes[i] = @as(u8, high) << 4 | @as(u8, low);
        }
        return .{ .bytes = bytes };
    }

    fn hexDigit(c: u8) ?u4 {
        return switch (c) {
            '0'...'9' => @intCast(c - '0'),
            'a'...'f' => @intCast(c - 'a' + 10),
            'A'...'F' => @intCast(c - 'A' + 10),
            else => null,
        };
    }

    /// Format UUID as string: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    pub fn toString(self: *const Uuid) [36]u8 {
        const hex_chars = "0123456789abcdef";
        var result: [36]u8 = undefined;
        var out_idx: usize = 0;
        for (0..16) |i| {
            if (i == 4 or i == 6 or i == 8 or i == 10) {
                result[out_idx] = '-';
                out_idx += 1;
            }
            result[out_idx] = hex_chars[self.bytes[i] >> 4];
            out_idx += 1;
            result[out_idx] = hex_chars[self.bytes[i] & 0x0F];
            out_idx += 1;
        }
        return result;
    }

    /// Check if this is the zero UUID.
    pub fn isZero(self: *const Uuid) bool {
        return std.mem.eql(u8, &self.bytes, &ZERO.bytes);
    }

    /// Compare two UUIDs for equality.
    pub fn eql(self: *const Uuid, other: *const Uuid) bool {
        return std.mem.eql(u8, &self.bytes, &other.bytes);
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "UUID random generation" {
    const id1 = Uuid.random();
    const id2 = Uuid.random();

    // Two random UUIDs should not be equal
    try testing.expect(!id1.eql(&id2));
    // Neither should be zero
    try testing.expect(!id1.isZero());
    try testing.expect(!id2.isZero());

    // Version 4 check
    try testing.expectEqual(@as(u4, 4), @as(u4, @truncate(id1.bytes[6] >> 4)));
    // Variant 1 check
    try testing.expectEqual(@as(u2, 2), @as(u2, @truncate(id1.bytes[8] >> 6)));
}

test "UUID zero" {
    try testing.expect(Uuid.ZERO.isZero());
}

test "UUID toString and fromString round-trip" {
    const id = Uuid.random();
    const str = id.toString();
    const parsed = try Uuid.fromStringSimple(&str);
    try testing.expect(id.eql(&parsed));
}

test "UUID fromBytes" {
    const bytes = [16]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 };
    const id = Uuid.fromBytes(bytes);
    try testing.expectEqualSlices(u8, &bytes, &id.bytes);
}

test "UUID known string" {
    const id = try Uuid.fromStringSimple("550e8400-e29b-41d4-a716-446655440000");
    const expected = [16]u8{ 0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00 };
    try testing.expectEqualSlices(u8, &expected, &id.bytes);
}

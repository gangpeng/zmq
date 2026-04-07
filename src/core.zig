/// AutoMQ Core Utilities
///
/// Foundational types and functions used across all modules:
/// - ByteBuffer: network-byte-order read/write cursors
/// - Varint: Kafka-style unsigned varint and signed zigzag encoding
/// - CRC32C: hardware-accelerated CRC-32C checksums
/// - UUID: 128-bit UUID generation and handling
/// - Time: monotonic and wall-clock time utilities

pub const byte_buffer = @import("core/byte_buffer.zig");
pub const varint = @import("core/varint.zig");
pub const crc32c = @import("core/crc32c.zig");
pub const uuid = @import("core/uuid.zig");
pub const time_utils = @import("core/time_utils.zig");

pub const ByteBuffer = byte_buffer.ByteBuffer;
pub const Varint = varint;
pub const Crc32c = crc32c;
pub const Uuid = uuid.Uuid;

test {
    @import("std").testing.refAllDecls(@This());
}

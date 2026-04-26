const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const crc32c = @import("core").crc32c;

/// Kafka Record Batch format.
///
/// The record batch is the fundamental unit of data in Kafka. It contains
/// a header with metadata and one or more records (key-value pairs).
///
/// Magic V2 (current, from Kafka 0.11+) format:
///
/// ```
/// baseOffset: int64                 (8 bytes)
/// batchLength: int32                (4 bytes) — length of everything after this field
/// partitionLeaderEpoch: int32       (4 bytes)
/// magic: int8                       (1 byte, = 2)
/// crc: uint32                       (4 bytes, CRC-32C of everything after crc)
/// attributes: int16                 (2 bytes)
/// lastOffsetDelta: int32            (4 bytes)
/// baseTimestamp: int64              (8 bytes)
/// maxTimestamp: int64               (8 bytes)
/// producerId: int64                 (8 bytes)
/// producerEpoch: int16              (2 bytes)
/// baseSequence: int32               (4 bytes)
/// records: [Record...]              (variable)
/// ```
///
/// Record format (V2, varint-encoded):
/// ```
/// length: varint                    (record length)
/// attributes: int8                  (unused, reserved)
/// timestampDelta: varlong           (delta from baseTimestamp)
/// offsetDelta: varint               (delta from baseOffset)
/// keyLength: varint                 (-1 for null)
/// key: bytes
/// valueLength: varint               (-1 for null)
/// value: bytes
/// headersCount: varint
/// headers: [Header...]
/// ```

// ---------------------------------------------------------------
// Record Batch Header
// ---------------------------------------------------------------

/// Offset of the CRC field within the batch (after baseOffset + batchLength + partitionLeaderEpoch + magic)
pub const CRC_OFFSET = 8 + 4 + 4 + 1; // = 17
/// Offset of the data that the CRC covers (everything after the CRC field)
pub const POST_CRC_OFFSET = CRC_OFFSET + 4; // = 21
/// Minimum batch header size (without records)
pub const BATCH_HEADER_SIZE = 61;

/// Batch attributes bit flags.
pub const Attributes = struct {
    /// Compression type (bits 0-2)
    pub const COMPRESSION_MASK: i16 = 0x07;
    /// Timestamp type (bit 3): 0 = CreateTime, 1 = LogAppendTime
    pub const TIMESTAMP_TYPE_MASK: i16 = 0x08;
    /// Is transactional (bit 4)
    pub const TRANSACTIONAL_MASK: i16 = 0x10;
    /// Is control batch (bit 5)
    pub const CONTROL_MASK: i16 = 0x20;
    /// Has delete horizon (bit 6)
    pub const DELETE_HORIZON_MASK: i16 = 0x40;
};

/// Parsed record batch header.
pub const RecordBatchHeader = struct {
    base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    record_count: i32,

    /// Parse a record batch header from raw bytes.
    /// The buffer must contain at least BATCH_HEADER_SIZE bytes.
    pub fn parse(buf: []const u8) !RecordBatchHeader {
        if (buf.len < BATCH_HEADER_SIZE) return error.BufferUnderflow;

        var pos: usize = 0;
        const base_offset = readI64(buf, &pos);
        const batch_length = readI32(buf, &pos);
        const partition_leader_epoch = readI32(buf, &pos);
        const magic = readI8(buf, &pos);

        if (magic != 0 and magic != 1 and magic != 2) return error.UnsupportedMagic;

        // V0 and V1 have a simpler header format
        if (magic <= 1) {
            // V0/V1: baseOffset(8) + batchLength(4) + crc(4) + magic(1) + attributes(1) + [timestamp(8) for V1] + key + value
            // We parse what we can and fill defaults for V2-only fields
            const crc_v0 = readU32(buf, &pos);
            const attr_byte = readI8(buf, &pos);
            const attributes_v0: i16 = @intCast(attr_byte);
            var timestamp: i64 = -1;
            if (magic == 1) {
                timestamp = readI64(buf, &pos);
            }
            return .{
                .base_offset = base_offset,
                .batch_length = batch_length,
                .partition_leader_epoch = partition_leader_epoch,
                .magic = magic,
                .crc = crc_v0,
                .attributes = attributes_v0,
                .last_offset_delta = 0,
                .base_timestamp = timestamp,
                .max_timestamp = timestamp,
                .producer_id = -1,
                .producer_epoch = -1,
                .base_sequence = -1,
                .record_count = 1, // V0/V1 batches contain a single message
            };
        }

        const crc = readU32(buf, &pos);
        const attributes = readI16(buf, &pos);
        const last_offset_delta = readI32(buf, &pos);
        const base_timestamp = readI64(buf, &pos);
        const max_timestamp = readI64(buf, &pos);
        const producer_id = readI64(buf, &pos);
        const producer_epoch = readI16(buf, &pos);
        const base_sequence = readI32(buf, &pos);
        const record_count = readI32(buf, &pos);

        return .{
            .base_offset = base_offset,
            .batch_length = batch_length,
            .partition_leader_epoch = partition_leader_epoch,
            .magic = magic,
            .crc = crc,
            .attributes = attributes,
            .last_offset_delta = last_offset_delta,
            .base_timestamp = base_timestamp,
            .max_timestamp = max_timestamp,
            .producer_id = producer_id,
            .producer_epoch = producer_epoch,
            .base_sequence = base_sequence,
            .record_count = record_count,
        };
    }

    /// Write the batch header to the buffer. CRC is NOT computed here — call
    /// computeAndSetCrc() after writing all records.
    pub fn write(self: *const RecordBatchHeader, buf: []u8) void {
        var pos: usize = 0;
        writeI64(buf, &pos, self.base_offset);
        writeI32(buf, &pos, self.batch_length);
        writeI32(buf, &pos, self.partition_leader_epoch);
        writeI8(buf, &pos, self.magic);
        writeU32(buf, &pos, self.crc);
        writeI16(buf, &pos, self.attributes);
        writeI32(buf, &pos, self.last_offset_delta);
        writeI64(buf, &pos, self.base_timestamp);
        writeI64(buf, &pos, self.max_timestamp);
        writeI64(buf, &pos, self.producer_id);
        writeI16(buf, &pos, self.producer_epoch);
        writeI32(buf, &pos, self.base_sequence);
        writeI32(buf, &pos, self.record_count);
    }

    /// Get the compression type from attributes.
    pub fn compressionType(self: *const RecordBatchHeader) u3 {
        return @truncate(@as(u16, @bitCast(self.attributes)) & 0x07);
    }

    /// Whether this batch is transactional.
    pub fn isTransactional(self: *const RecordBatchHeader) bool {
        return self.attributes & Attributes.TRANSACTIONAL_MASK != 0;
    }

    /// Whether this is a control batch.
    pub fn isControlBatch(self: *const RecordBatchHeader) bool {
        return self.attributes & Attributes.CONTROL_MASK != 0;
    }

    /// Whether the timestamp type is LogAppendTime.
    pub fn isLogAppendTime(self: *const RecordBatchHeader) bool {
        return self.attributes & Attributes.TIMESTAMP_TYPE_MASK != 0;
    }

    /// Total batch size including baseOffset and batchLength fields.
    pub fn totalBatchSize(self: *const RecordBatchHeader) usize {
        return @intCast(@as(i64, self.batch_length) + 12); // 8 (baseOffset) + 4 (batchLength) + batchLength
    }
};

/// Validate the CRC of a record batch.
/// The CRC covers everything from attributes through the end of records.
pub fn validateCrc(batch_data: []const u8) bool {
    if (batch_data.len < BATCH_HEADER_SIZE) return false;

    var pos: usize = CRC_OFFSET;
    const stored_crc = readU32(batch_data, &pos);
    const crc_data = batch_data[POST_CRC_OFFSET..];
    const computed_crc = crc32c.compute(crc_data);

    return stored_crc == computed_crc;
}

/// Compute and write the CRC into the batch buffer.
pub fn computeAndSetCrc(batch_data: []u8) void {
    const crc_data = batch_data[POST_CRC_OFFSET..];
    const computed_crc = crc32c.compute(crc_data);
    std.mem.writeInt(u32, batch_data[CRC_OFFSET..][0..4], computed_crc, .big);
}

// ---------------------------------------------------------------
// Record (V2)
// ---------------------------------------------------------------

/// A single record within a V2 record batch.
pub const Record = struct {
    attributes: i8 = 0, // currently unused
    timestamp_delta: i64 = 0,
    offset_delta: i32 = 0,
    key: ?[]const u8 = null,
    value: ?[]const u8 = null,
    headers: []const RecordHeader = &.{},

    pub const RecordHeader = struct {
        key: []const u8,
        value: ?[]const u8,
    };
};

/// Parse a single V2 record from the buffer starting at `pos`.
/// Uses varint/varlong encoding.
pub fn parseRecord(buf: []const u8, pos: *usize) !Record {
    const record_length = try readSignedVarint(buf, pos);
    _ = record_length; // We could validate this matches actual bytes consumed

    const attributes: i8 = @bitCast(buf[pos.*]);
    pos.* += 1;

    const timestamp_delta = try readSignedVarlong(buf, pos);
    const offset_delta: i32 = @intCast(try readSignedVarint(buf, pos));

    // Key
    const key_length = try readSignedVarint(buf, pos);
    const key: ?[]const u8 = if (key_length < 0) null else blk: {
        const len: usize = @intCast(key_length);
        if (pos.* + len > buf.len) return error.BufferUnderflow;
        const k = buf[pos.* .. pos.* + len];
        pos.* += len;
        break :blk k;
    };

    // Value
    const value_length = try readSignedVarint(buf, pos);
    const value: ?[]const u8 = if (value_length < 0) null else blk: {
        const len: usize = @intCast(value_length);
        if (pos.* + len > buf.len) return error.BufferUnderflow;
        const v = buf[pos.* .. pos.* + len];
        pos.* += len;
        break :blk v;
    };

    // Headers
    const headers_count = try readSignedVarint(buf, pos);
    const num_headers: usize = if (headers_count < 0) 0 else @intCast(headers_count);

    // Skip headers (they're variable-length key-value pairs)
    // Parse each header: [varint key_len] [key] [varint value_len] [value]
    for (0..num_headers) |_| {
        // Header key
        const hdr_key_len = try readSignedVarint(buf, pos);
        if (hdr_key_len > 0) {
            const hkl: usize = @intCast(hdr_key_len);
            if (pos.* + hkl > buf.len) return error.BufferUnderflow;
            pos.* += hkl;
        }
        // Header value
        const hdr_val_len = try readSignedVarint(buf, pos);
        if (hdr_val_len > 0) {
            const hvl: usize = @intCast(hdr_val_len);
            if (pos.* + hvl > buf.len) return error.BufferUnderflow;
            pos.* += hvl;
        }
    }

    return .{
        .attributes = attributes,
        .timestamp_delta = timestamp_delta,
        .offset_delta = offset_delta,
        .key = key,
        .value = value,
    };
}

/// Write a single V2 record to the buffer.
/// Returns the number of bytes written.
pub fn writeRecord(buf: []u8, pos: *usize, record: *const Record) usize {
    const start_pos = pos.*;

    // Calculate record body size first (everything after the length varint)
    const body_size = recordBodySize(record);

    // Write record length as signed varint
    writeSignedVarint(buf, pos, @intCast(body_size));

    // Attributes
    buf[pos.*] = @bitCast(record.attributes);
    pos.* += 1;

    // Timestamp delta
    writeSignedVarlong(buf, pos, record.timestamp_delta);

    // Offset delta
    writeSignedVarint(buf, pos, record.offset_delta);

    // Key
    if (record.key) |key| {
        writeSignedVarint(buf, pos, @intCast(key.len));
        @memcpy(buf[pos.* .. pos.* + key.len], key);
        pos.* += key.len;
    } else {
        writeSignedVarint(buf, pos, -1);
    }

    // Value
    if (record.value) |value| {
        writeSignedVarint(buf, pos, @intCast(value.len));
        @memcpy(buf[pos.* .. pos.* + value.len], value);
        pos.* += value.len;
    } else {
        writeSignedVarint(buf, pos, -1);
    }

    // Headers count
    writeSignedVarint(buf, pos, @intCast(record.headers.len));
    for (record.headers) |hdr| {
        writeSignedVarint(buf, pos, @intCast(hdr.key.len));
        @memcpy(buf[pos.* .. pos.* + hdr.key.len], hdr.key);
        pos.* += hdr.key.len;
        if (hdr.value) |val| {
            writeSignedVarint(buf, pos, @intCast(val.len));
            @memcpy(buf[pos.* .. pos.* + val.len], val);
            pos.* += val.len;
        } else {
            writeSignedVarint(buf, pos, -1);
        }
    }

    return pos.* - start_pos;
}

fn recordBodySize(record: *const Record) usize {
    var size: usize = 0;
    size += 1; // attributes
    size += signedVarlongSize(record.timestamp_delta);
    size += signedVarintSize(record.offset_delta);

    // Key
    if (record.key) |key| {
        size += signedVarintSize(@intCast(key.len));
        size += key.len;
    } else {
        size += 1; // varint -1
    }

    // Value
    if (record.value) |value| {
        size += signedVarintSize(@intCast(value.len));
        size += value.len;
    } else {
        size += 1;
    }

    // Headers
    size += signedVarintSize(@intCast(record.headers.len));
    for (record.headers) |hdr| {
        size += signedVarintSize(@intCast(hdr.key.len));
        size += hdr.key.len;
        if (hdr.value) |val| {
            size += signedVarintSize(@intCast(val.len));
            size += val.len;
        } else {
            size += 1;
        }
    }

    return size;
}

// ---------------------------------------------------------------
// Record Batch Builder
// ---------------------------------------------------------------

/// Build a complete V2 record batch from individual records.
pub fn buildRecordBatch(
    alloc: Allocator,
    base_offset: i64,
    records: []const Record,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    attributes: i16,
) ![]u8 {
    // First, serialize all records to compute their total size
    var records_buf = try alloc.alloc(u8, records.len * 256); // generous estimate
    defer alloc.free(records_buf);

    var records_pos: usize = 0;
    for (records) |*record| {
        _ = writeRecord(records_buf, &records_pos, record);
    }

    // Compute batch_length: everything after baseOffset(8) + batchLength(4)
    const batch_length: i32 = @intCast(BATCH_HEADER_SIZE - 12 + records_pos);

    // Allocate final buffer
    const total_size: usize = @intCast(@as(i64, batch_length) + 12);
    var batch_buf = try alloc.alloc(u8, total_size);
    errdefer alloc.free(batch_buf);

    // Write header
    const last_offset_delta: i32 = if (records.len > 0) @intCast(records.len - 1) else 0;
    const header_val = RecordBatchHeader{
        .base_offset = base_offset,
        .batch_length = batch_length,
        .partition_leader_epoch = 0,
        .magic = 2,
        .crc = 0, // placeholder
        .attributes = attributes,
        .last_offset_delta = last_offset_delta,
        .base_timestamp = base_timestamp,
        .max_timestamp = max_timestamp,
        .producer_id = producer_id,
        .producer_epoch = producer_epoch,
        .base_sequence = base_sequence,
        .record_count = @intCast(records.len),
    };
    header_val.write(batch_buf);

    // Copy records
    @memcpy(batch_buf[BATCH_HEADER_SIZE .. BATCH_HEADER_SIZE + records_pos], records_buf[0..records_pos]);

    // Compute and set CRC
    computeAndSetCrc(batch_buf);

    return batch_buf;
}

// ---------------------------------------------------------------
// Varint helpers (signed zigzag, for record fields)
// ---------------------------------------------------------------

fn readSignedVarint(buf: []const u8, pos: *usize) !i32 {
    var result: u32 = 0;
    var shift: u5 = 0;
    for (0..5) |_| {
        if (pos.* >= buf.len) return error.BufferUnderflow;
        const byte = buf[pos.*];
        pos.* += 1;
        result |= @as(u32, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) {
            // Zigzag decode
            return @as(i32, @bitCast((result >> 1))) ^ -@as(i32, @bitCast(result & 1));
        }
        shift +%= 7;
    }
    return error.VarintTooLong;
}

fn readSignedVarlong(buf: []const u8, pos: *usize) !i64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    for (0..10) |_| {
        if (pos.* >= buf.len) return error.BufferUnderflow;
        const byte = buf[pos.*];
        pos.* += 1;
        result |= @as(u64, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) {
            return @as(i64, @bitCast((result >> 1))) ^ -@as(i64, @bitCast(result & 1));
        }
        shift +%= 7;
    }
    return error.VarintTooLong;
}

fn writeSignedVarint(buf: []u8, pos: *usize, value: i32) void {
    const zigzag: u32 = @bitCast((value << 1) ^ (value >> 31));
    writeUnsignedVarint32(buf, pos, zigzag);
}

fn writeSignedVarlong(buf: []u8, pos: *usize, value: i64) void {
    const zigzag: u64 = @bitCast((value << 1) ^ (value >> 63));
    writeUnsignedVarint64(buf, pos, zigzag);
}

fn writeUnsignedVarint32(buf: []u8, pos: *usize, value: u32) void {
    var v = value;
    while (v > 0x7F) {
        buf[pos.*] = @as(u8, @truncate(v & 0x7F)) | 0x80;
        pos.* += 1;
        v >>= 7;
    }
    buf[pos.*] = @as(u8, @truncate(v));
    pos.* += 1;
}

fn writeUnsignedVarint64(buf: []u8, pos: *usize, value: u64) void {
    var v = value;
    while (v > 0x7F) {
        buf[pos.*] = @as(u8, @truncate(v & 0x7F)) | 0x80;
        pos.* += 1;
        v >>= 7;
    }
    buf[pos.*] = @as(u8, @truncate(v));
    pos.* += 1;
}

fn signedVarintSize(value: i32) usize {
    const zigzag: u32 = @bitCast((value << 1) ^ (value >> 31));
    return unsignedVarint32Size(zigzag);
}

fn signedVarlongSize(value: i64) usize {
    const zigzag: u64 = @bitCast((value << 1) ^ (value >> 63));
    return unsignedVarint64Size(zigzag);
}

fn unsignedVarint32Size(value: u32) usize {
    if (value < (1 << 7)) return 1;
    if (value < (1 << 14)) return 2;
    if (value < (1 << 21)) return 3;
    if (value < (1 << 28)) return 4;
    return 5;
}

fn unsignedVarint64Size(value: u64) usize {
    var v = value;
    var size: usize = 1;
    while (v > 0x7F) : (size += 1) v >>= 7;
    return size;
}

// Big-endian read helpers
fn readI8(buf: []const u8, pos: *usize) i8 {
    const val: i8 = @bitCast(buf[pos.*]);
    pos.* += 1;
    return val;
}

fn readI16(buf: []const u8, pos: *usize) i16 {
    const val = std.mem.readInt(i16, buf[pos.*..][0..2], .big);
    pos.* += 2;
    return val;
}

fn readI32(buf: []const u8, pos: *usize) i32 {
    const val = std.mem.readInt(i32, buf[pos.*..][0..4], .big);
    pos.* += 4;
    return val;
}

fn readU32(buf: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, buf[pos.*..][0..4], .big);
    pos.* += 4;
    return val;
}

fn readI64(buf: []const u8, pos: *usize) i64 {
    const val = std.mem.readInt(i64, buf[pos.*..][0..8], .big);
    pos.* += 8;
    return val;
}

fn writeI8(buf: []u8, pos: *usize, val: i8) void {
    buf[pos.*] = @bitCast(val);
    pos.* += 1;
}

fn writeI16(buf: []u8, pos: *usize, val: i16) void {
    std.mem.writeInt(i16, buf[pos.*..][0..2], val, .big);
    pos.* += 2;
}

fn writeI32(buf: []u8, pos: *usize, val: i32) void {
    std.mem.writeInt(i32, buf[pos.*..][0..4], val, .big);
    pos.* += 4;
}

fn writeU32(buf: []u8, pos: *usize, val: u32) void {
    std.mem.writeInt(u32, buf[pos.*..][0..4], val, .big);
    pos.* += 4;
}

fn writeI64(buf: []u8, pos: *usize, val: i64) void {
    std.mem.writeInt(i64, buf[pos.*..][0..8], val, .big);
    pos.* += 8;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "RecordBatchHeader parse and write round-trip" {
    var buf: [BATCH_HEADER_SIZE]u8 = undefined;

    const original = RecordBatchHeader{
        .base_offset = 42,
        .batch_length = 100,
        .partition_leader_epoch = 5,
        .magic = 2,
        .crc = 0x12345678,
        .attributes = 0,
        .last_offset_delta = 3,
        .base_timestamp = 1704067200000,
        .max_timestamp = 1704067201000,
        .producer_id = -1,
        .producer_epoch = -1,
        .base_sequence = -1,
        .record_count = 4,
    };
    original.write(&buf);

    const parsed = try RecordBatchHeader.parse(&buf);
    try testing.expectEqual(original.base_offset, parsed.base_offset);
    try testing.expectEqual(original.batch_length, parsed.batch_length);
    try testing.expectEqual(original.magic, parsed.magic);
    try testing.expectEqual(original.crc, parsed.crc);
    try testing.expectEqual(original.attributes, parsed.attributes);
    try testing.expectEqual(original.last_offset_delta, parsed.last_offset_delta);
    try testing.expectEqual(original.base_timestamp, parsed.base_timestamp);
    try testing.expectEqual(original.max_timestamp, parsed.max_timestamp);
    try testing.expectEqual(original.producer_id, parsed.producer_id);
    try testing.expectEqual(original.producer_epoch, parsed.producer_epoch);
    try testing.expectEqual(original.base_sequence, parsed.base_sequence);
    try testing.expectEqual(original.record_count, parsed.record_count);
}

test "Record write and parse round-trip" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    const record = Record{
        .attributes = 0,
        .timestamp_delta = 100,
        .offset_delta = 0,
        .key = "my-key",
        .value = "my-value",
    };
    _ = writeRecord(&buf, &wpos, &record);

    var rpos: usize = 0;
    const parsed = try parseRecord(&buf, &rpos);

    try testing.expectEqual(@as(i8, 0), parsed.attributes);
    try testing.expectEqual(@as(i64, 100), parsed.timestamp_delta);
    try testing.expectEqual(@as(i32, 0), parsed.offset_delta);
    try testing.expectEqualStrings("my-key", parsed.key.?);
    try testing.expectEqualStrings("my-value", parsed.value.?);
    try testing.expectEqual(wpos, rpos);
}

test "Record with null key" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    const record = Record{
        .offset_delta = 1,
        .value = "just a value",
    };
    _ = writeRecord(&buf, &wpos, &record);

    var rpos: usize = 0;
    const parsed = try parseRecord(&buf, &rpos);

    try testing.expect(parsed.key == null);
    try testing.expectEqualStrings("just a value", parsed.value.?);
}

test "buildRecordBatch creates valid batch with correct CRC" {
    const records = [_]Record{
        .{ .offset_delta = 0, .key = "k0", .value = "v0" },
        .{ .offset_delta = 1, .timestamp_delta = 10, .key = "k1", .value = "v1" },
        .{ .offset_delta = 2, .timestamp_delta = 20, .key = "k2", .value = "v2" },
    };

    const batch = try buildRecordBatch(
        testing.allocator,
        0, // base_offset
        &records,
        -1, // producer_id
        -1, // producer_epoch
        -1, // base_sequence
        1704067200000, // base_timestamp
        1704067200020, // max_timestamp
        0, // attributes (no compression)
    );
    defer testing.allocator.free(batch);

    // Validate CRC
    try testing.expect(validateCrc(batch));

    // Parse the header
    const header = try RecordBatchHeader.parse(batch);
    try testing.expectEqual(@as(i8, 2), header.magic);
    try testing.expectEqual(@as(i32, 3), header.record_count);
    try testing.expectEqual(@as(i32, 2), header.last_offset_delta);
    try testing.expectEqual(@as(i64, 0), header.base_offset);

    // Parse individual records
    var pos: usize = BATCH_HEADER_SIZE;
    for (0..3) |i| {
        const rec = try parseRecord(batch, &pos);
        try testing.expectEqual(@as(i32, @intCast(i)), rec.offset_delta);
    }
}

test "RecordBatchHeader attribute helpers" {
    const header = RecordBatchHeader{
        .base_offset = 0,
        .batch_length = 0,
        .partition_leader_epoch = 0,
        .magic = 2,
        .crc = 0,
        .attributes = 0b0011_0100, // zstd(4) + transactional + control
        .last_offset_delta = 0,
        .base_timestamp = 0,
        .max_timestamp = 0,
        .producer_id = 0,
        .producer_epoch = 0,
        .base_sequence = 0,
        .record_count = 0,
    };

    try testing.expectEqual(@as(u3, 4), header.compressionType()); // zstd
    try testing.expect(header.isTransactional());
    try testing.expect(header.isControlBatch());
    try testing.expect(!header.isLogAppendTime());
}

test "CRC corruption detected" {
    const records = [_]Record{
        .{ .offset_delta = 0, .value = "test" },
    };

    const batch = try buildRecordBatch(
        testing.allocator,
        0,
        &records,
        -1,
        -1,
        -1,
        0,
        0,
        0,
    );
    defer testing.allocator.free(batch);

    // Valid CRC
    try testing.expect(validateCrc(batch));

    // Corrupt a byte in the data area
    batch[BATCH_HEADER_SIZE + 2] ^= 0xFF;
    try testing.expect(!validateCrc(batch));
}

test "buildRecordBatch and iterate round-trip" {
    const records = [_]Record{
        .{ .offset_delta = 0, .value = "hello" },
        .{ .offset_delta = 1, .value = "world" },
        .{ .offset_delta = 2, .key = "key", .value = "val" },
    };

    const batch = try buildRecordBatch(
        testing.allocator,
        100, // base_offset
        &records,
        42, // producer_id
        1, // producer_epoch
        0, // base_sequence
        1000, // base_timestamp
        2000, // max_timestamp
        0, // attributes (none)
    );
    defer testing.allocator.free(batch);

    // Validate CRC
    try testing.expect(validateCrc(batch));

    // Parse header
    const header = try RecordBatchHeader.parse(batch);
    try testing.expectEqual(@as(i64, 100), header.base_offset);
    try testing.expectEqual(@as(i32, 3), header.record_count);
    try testing.expectEqual(@as(i8, 2), header.magic);
    try testing.expectEqual(@as(i64, 42), header.producer_id);

    // Iterate records by parsing from data after header
    var pos: usize = BATCH_HEADER_SIZE;

    const r0 = try parseRecord(batch, &pos);
    try testing.expectEqualStrings("hello", r0.value.?);

    const r1 = try parseRecord(batch, &pos);
    try testing.expectEqualStrings("world", r1.value.?);

    const r2 = try parseRecord(batch, &pos);
    try testing.expectEqualStrings("val", r2.value.?);
    try testing.expectEqualStrings("key", r2.key.?);
}

test "RecordBatchHeader.parse buffer underflow" {
    var short_buf: [10]u8 = undefined;
    const result = RecordBatchHeader.parse(&short_buf);
    try testing.expectError(error.BufferUnderflow, result);
}

test "RecordBatchHeader.parse unsupported magic" {
    var buf: [BATCH_HEADER_SIZE]u8 = undefined;
    // Set a valid-looking header but with magic = 3 (unsupported)
    const header_val = RecordBatchHeader{
        .base_offset = 0,
        .batch_length = 0,
        .partition_leader_epoch = 0,
        .magic = 2,
        .crc = 0,
        .attributes = 0,
        .last_offset_delta = 0,
        .base_timestamp = 0,
        .max_timestamp = 0,
        .producer_id = 0,
        .producer_epoch = 0,
        .base_sequence = 0,
        .record_count = 0,
    };
    header_val.write(&buf);
    // Overwrite magic byte to 3
    buf[16] = 3; // magic is at offset 16 (8+4+4)
    const result = RecordBatchHeader.parse(&buf);
    try testing.expectError(error.UnsupportedMagic, result);
}

test "Record with null value" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    const record = Record{
        .offset_delta = 0,
        .key = "key-only",
        .value = null,
    };
    _ = writeRecord(&buf, &wpos, &record);

    var rpos: usize = 0;
    const parsed = try parseRecord(&buf, &rpos);

    try testing.expectEqualStrings("key-only", parsed.key.?);
    try testing.expect(parsed.value == null);
    try testing.expectEqual(wpos, rpos);
}

test "Record with both null key and null value" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    const record = Record{
        .offset_delta = 5,
        .timestamp_delta = 42,
    };
    _ = writeRecord(&buf, &wpos, &record);

    var rpos: usize = 0;
    const parsed = try parseRecord(&buf, &rpos);

    try testing.expect(parsed.key == null);
    try testing.expect(parsed.value == null);
    try testing.expectEqual(@as(i32, 5), parsed.offset_delta);
    try testing.expectEqual(@as(i64, 42), parsed.timestamp_delta);
}

test "Record with headers" {
    var buf: [512]u8 = undefined;
    var wpos: usize = 0;

    const headers = [_]Record.RecordHeader{
        .{ .key = "h1", .value = "v1" },
        .{ .key = "h2", .value = null },
    };

    const record = Record{
        .offset_delta = 0,
        .key = "k",
        .value = "v",
        .headers = &headers,
    };
    _ = writeRecord(&buf, &wpos, &record);

    var rpos: usize = 0;
    const parsed = try parseRecord(&buf, &rpos);

    try testing.expectEqualStrings("k", parsed.key.?);
    try testing.expectEqualStrings("v", parsed.value.?);
    // Note: parsed record doesn't return headers (they're skipped), but parsing should succeed
    try testing.expectEqual(wpos, rpos);
}

test "validateCrc with too-short buffer returns false" {
    var short_buf: [10]u8 = undefined;
    try testing.expect(!validateCrc(&short_buf));
}

test "buildRecordBatch single record" {
    const records = [_]Record{
        .{ .offset_delta = 0, .key = "single-key", .value = "single-value" },
    };

    const batch = try buildRecordBatch(
        testing.allocator,
        0,
        &records,
        -1,
        -1,
        -1,
        0,
        0,
        0,
    );
    defer testing.allocator.free(batch);

    try testing.expect(validateCrc(batch));
    const header = try RecordBatchHeader.parse(batch);
    try testing.expectEqual(@as(i32, 1), header.record_count);
    try testing.expectEqual(@as(i32, 0), header.last_offset_delta);
}

test "computeAndSetCrc makes CRC valid" {
    const records = [_]Record{
        .{ .offset_delta = 0, .value = "test" },
    };

    const batch = try buildRecordBatch(
        testing.allocator,
        0,
        &records,
        -1,
        -1,
        -1,
        0,
        0,
        0,
    );
    defer testing.allocator.free(batch);

    // Zero out the CRC
    std.mem.writeInt(u32, batch[CRC_OFFSET..][0..4], 0, .big);
    try testing.expect(!validateCrc(batch));

    // Recompute
    computeAndSetCrc(batch);
    try testing.expect(validateCrc(batch));
}

test "RecordBatchHeader.totalBatchSize" {
    const header = RecordBatchHeader{
        .base_offset = 0,
        .batch_length = 100,
        .partition_leader_epoch = 0,
        .magic = 2,
        .crc = 0,
        .attributes = 0,
        .last_offset_delta = 0,
        .base_timestamp = 0,
        .max_timestamp = 0,
        .producer_id = 0,
        .producer_epoch = 0,
        .base_sequence = 0,
        .record_count = 0,
    };
    // totalBatchSize = batch_length + 12 (8 for baseOffset + 4 for batchLength)
    try testing.expectEqual(@as(usize, 112), header.totalBatchSize());
}

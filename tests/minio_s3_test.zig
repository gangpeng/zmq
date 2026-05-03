const std = @import("std");
const testing = std.testing;

const storage = @import("storage");
const broker = @import("broker");
const time = @import("time_compat");

fn getenv(name: [:0]const u8) ?[]const u8 {
    const value = std.c.getenv(name.ptr) orelse return null;
    return std.mem.span(value);
}

fn envOr(name: [:0]const u8, default: []const u8) []const u8 {
    return getenv(name) orelse default;
}

fn envBool(name: [:0]const u8, default: bool) bool {
    const value = getenv(name) orelse return default;
    if (std.mem.eql(u8, value, "1") or std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "yes")) {
        return true;
    }
    if (std.mem.eql(u8, value, "0") or std.ascii.eqlIgnoreCase(value, "false") or std.ascii.eqlIgnoreCase(value, "no")) {
        return false;
    }
    return default;
}

fn envScheme(name: [:0]const u8, default: storage.S3Client.Scheme) !storage.S3Client.Scheme {
    const value = getenv(name) orelse return default;
    if (std.ascii.eqlIgnoreCase(value, "http")) return .http;
    if (std.ascii.eqlIgnoreCase(value, "https")) return .https;
    return error.InvalidMinioScheme;
}

fn requireMinioConfig() !storage.S3Client.Config {
    const enabled = getenv("ZMQ_RUN_MINIO_TESTS") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, enabled, "1") and !std.ascii.eqlIgnoreCase(enabled, "true")) {
        return error.SkipZigTest;
    }

    const port_text = envOr("ZMQ_S3_PORT", "9000");
    const port = std.fmt.parseInt(u16, port_text, 10) catch return error.InvalidMinioPort;

    return .{
        .host = envOr("ZMQ_S3_ENDPOINT", "127.0.0.1"),
        .port = port,
        .bucket = envOr("ZMQ_S3_BUCKET", "zmq-minio-it"),
        .access_key = envOr("ZMQ_S3_ACCESS_KEY", "minioadmin"),
        .secret_key = envOr("ZMQ_S3_SECRET_KEY", "minioadmin"),
        .scheme = try envScheme("ZMQ_S3_SCHEME", .http),
        .region = envOr("ZMQ_S3_REGION", "us-east-1"),
        .path_style = envBool("ZMQ_S3_PATH_STYLE", true),
        .tls_ca_file = getenv("ZMQ_S3_TLS_CA_FILE"),
    };
}

fn initMinioClient() !storage.S3Client {
    var client = storage.S3Client.init(testing.allocator, try requireMinioConfig());
    if (!envBool("ZMQ_S3_SKIP_ENSURE_BUCKET", false)) {
        try client.ensureBucket();
    }
    return client;
}

fn uniqueKey(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{d}", .{ prefix, time.nanoTimestamp() });
}

fn freeKeys(alloc: std.mem.Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

fn cleanupPrefix(s3: *storage.S3Storage, prefix: []const u8) void {
    const keys = s3.listObjectKeys(prefix) catch return;
    defer freeKeys(testing.allocator, keys);

    for (keys) |key| {
        s3.deleteObject(key) catch {};
    }
}

fn allocMultipartData(alloc: std.mem.Allocator) ![]u8 {
    const data = try alloc.alloc(u8, 5 * 1024 * 1024 + 257);
    for (data, 0..) |*byte, i| {
        byte.* = @intCast(i % 251);
    }
    return data;
}

fn requireProviderGate(name: [:0]const u8) !void {
    if (!envBool(name, false)) return error.SkipZigTest;
    _ = try requireMinioConfig();
}

test "MinIO S3Storage object writer get range list delete round-trip" {
    var client = try initMinioClient();
    var s3 = storage.S3Storage.initReal(testing.allocator, &client);

    const key = try uniqueKey(testing.allocator, "integration/object");
    defer testing.allocator.free(key);
    defer s3.deleteObject(key) catch {};

    var writer = storage.ObjectWriter.init(testing.allocator);
    defer writer.deinit();
    try writer.addDataBlock(11, 0, 1, 1, "alpha");
    try writer.addDataBlock(11, 1, 1, 1, "bravo");
    const object_data = try writer.build();
    defer testing.allocator.free(object_data);

    try s3.putObject(key, object_data);

    const fetched = (try s3.getObject(key)).?;
    defer testing.allocator.free(fetched);

    var reader = try storage.ObjectReader.parse(testing.allocator, fetched);
    defer reader.deinit();
    try testing.expect(reader.has_checksum);
    try testing.expectEqualStrings("alpha", reader.readBlock(0).?);
    try testing.expectEqualStrings("bravo", reader.readBlock(1).?);

    const ranged = (try s3.getObjectRange(key, 0, 5)).?;
    defer testing.allocator.free(ranged);
    try testing.expectEqualStrings("alpha", ranged);

    const keys = try s3.listObjectKeys("integration/object");
    defer freeKeys(testing.allocator, keys);
    var found = false;
    for (keys) |listed| {
        if (std.mem.eql(u8, listed, key)) found = true;
    }
    try testing.expect(found);

    try s3.deleteObject(key);
    try testing.expect((s3.getObject(key) catch null) == null);
}

test "MinIO S3Client multipart round-trip" {
    var client = try initMinioClient();

    const key = try uniqueKey(testing.allocator, "integration/multipart");
    defer testing.allocator.free(key);
    defer client.deleteObject(key) catch {};

    const data = try allocMultipartData(testing.allocator);
    defer testing.allocator.free(data);

    try client.putObjectMultipart(key, data);

    const fetched = try client.getObject(key);
    defer testing.allocator.free(fetched);
    try testing.expectEqual(data.len, fetched.len);
    try testing.expectEqualSlices(u8, data, fetched);

    try client.deleteObject(key);
}

test "MinIO S3 WAL objects rebuild metadata and fetch records" {
    var client = try initMinioClient();
    var s3 = storage.S3Storage.initReal(testing.allocator, &client);
    cleanupPrefix(&s3, "wal/");
    defer cleanupPrefix(&s3, "wal/");

    const topic = "minio-rebuild-topic";
    const stream_id = broker.PartitionStore.hashPartitionKey(topic, 0);

    {
        var batcher = storage.wal.S3WalBatcher.init(testing.allocator);
        defer batcher.deinit();

        try batcher.append(stream_id, 0, "live-a");
        try batcher.append(stream_id, 1, "live-b");
        try testing.expect(batcher.flushNow(&s3));
    }

    var object_manager = storage.ObjectManager.init(testing.allocator, 1);
    defer object_manager.deinit();

    var store = broker.PartitionStore.init(testing.allocator);
    defer store.deinit();
    store.s3_storage = s3;
    store.object_manager = &object_manager;

    try testing.expectEqual(@as(u64, 1), try store.recoverS3WalObjects());
    try store.ensurePartition(topic, 0);
    try testing.expect(store.repairPartitionStatesFromObjectManager());

    const result = try store.fetch(topic, 0, 0, 1024);
    defer if (result.records.len > 0) testing.allocator.free(@constCast(result.records));
    try testing.expectEqual(@as(i16, 0), result.error_code);
    try testing.expectEqualStrings("live-alive-b", result.records);
}

test "MinIO PartitionStore S3 WAL produce rebuilds and resumes" {
    var client = try initMinioClient();
    var s3 = storage.S3Storage.initReal(testing.allocator, &client);
    cleanupPrefix(&s3, "wal/");
    defer cleanupPrefix(&s3, "wal/");

    const topic = "minio-store-resume-topic";

    {
        var producer_store = broker.PartitionStore.init(testing.allocator);
        defer producer_store.deinit();
        producer_store.s3_wal_mode = true;
        producer_store.s3_storage = s3;
        producer_store.s3_wal_batcher = storage.wal.S3WalBatcher.init(testing.allocator);

        const first = try producer_store.produce(topic, 0, "live-a");
        const second = try producer_store.produce(topic, 0, "live-b");
        try testing.expectEqual(@as(i64, 0), first.base_offset);
        try testing.expectEqual(@as(i64, 1), second.base_offset);
    }

    var object_manager = storage.ObjectManager.init(testing.allocator, 1);
    defer object_manager.deinit();

    var replacement_store = broker.PartitionStore.init(testing.allocator);
    defer replacement_store.deinit();
    replacement_store.s3_wal_mode = true;
    replacement_store.s3_storage = s3;
    replacement_store.s3_wal_batcher = storage.wal.S3WalBatcher.init(testing.allocator);
    replacement_store.setObjectManager(&object_manager);

    try testing.expectEqual(@as(u64, 2), try replacement_store.recoverS3WalObjects());
    try replacement_store.ensurePartition(topic, 0);
    try testing.expect(replacement_store.repairPartitionStatesFromObjectManager());
    try testing.expectEqual(@as(u64, 2), replacement_store.s3_wal_batcher.?.s3_object_counter);

    const recovered = try replacement_store.fetch(topic, 0, 0, 1024);
    defer if (recovered.records.len > 0) testing.allocator.free(@constCast(recovered.records));
    try testing.expectEqual(@as(i16, 0), recovered.error_code);
    try testing.expectEqualStrings("live-alive-b", recovered.records);

    const third = try replacement_store.produce(topic, 0, "live-c");
    try testing.expectEqual(@as(i64, 2), third.base_offset);

    const merged = try replacement_store.fetch(topic, 0, 0, 1024);
    defer if (merged.records.len > 0) testing.allocator.free(@constCast(merged.records));
    try testing.expectEqual(@as(i16, 0), merged.error_code);
    try testing.expectEqualStrings("live-alive-blive-c", merged.records);
}

test "MinIO/S3 provider ListObjects pagination gate" {
    try requireProviderGate("ZMQ_S3_REQUIRE_LIST_PAGINATION");

    var client = try initMinioClient();
    var s3 = storage.S3Storage.initReal(testing.allocator, &client);

    const prefix = try uniqueKey(testing.allocator, "integration/pagination");
    defer testing.allocator.free(prefix);
    defer cleanupPrefix(&s3, prefix);

    const object_count = 1005;
    var i: usize = 0;
    while (i < object_count) : (i += 1) {
        const key = try std.fmt.allocPrint(testing.allocator, "{s}/{d:0>4}", .{ prefix, i });
        defer testing.allocator.free(key);
        try s3.putObject(key, "x");
    }

    const keys = try s3.listObjectKeys(prefix);
    defer freeKeys(testing.allocator, keys);
    try testing.expectEqual(@as(usize, object_count), keys.len);
    try testing.expect(std.mem.endsWith(u8, keys[0], "/0000"));
    try testing.expect(std.mem.endsWith(u8, keys[keys.len - 1], "/1004"));
}

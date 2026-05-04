const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// AWS Signature Version 4 signing for S3-compatible APIs.
///
/// Implements the full SigV4 signing process:
/// 1. Create canonical request
/// 2. Create string to sign
/// 3. Calculate signing key
/// 4. Calculate signature
/// 5. Build Authorization header
pub const AwsSigV4 = struct {
    access_key: []const u8,
    secret_key: []const u8,
    region: []const u8,
    service: []const u8,

    pub fn init(access_key: []const u8, secret_key: []const u8) AwsSigV4 {
        return initWithRegion(access_key, secret_key, "us-east-1");
    }

    pub fn initWithRegion(access_key: []const u8, secret_key: []const u8, region: []const u8) AwsSigV4 {
        return .{
            .access_key = access_key,
            .secret_key = secret_key,
            .region = region,
            .service = "s3",
        };
    }

    /// Sign an HTTP request and return the Authorization header value.
    /// Also returns the x-amz-date and x-amz-content-sha256 headers that must be included.
    pub fn sign(
        self: *const AwsSigV4,
        alloc: Allocator,
        method: []const u8,
        path: []const u8,
        query: []const u8,
        host: []const u8,
        payload_hash: []const u8,
        date_stamp: []const u8, // "20260403"
        amz_date: []const u8, // "20260403T174100Z"
    ) ![]u8 {
        // Step 1: Canonical Request
        const canonical_headers = try std.fmt.allocPrint(alloc, "host:{s}\nx-amz-content-sha256:{s}\nx-amz-date:{s}\n", .{ host, payload_hash, amz_date });
        defer alloc.free(canonical_headers);

        const signed_headers = "host;x-amz-content-sha256;x-amz-date";

        const canonical_query = try canonicalQueryString(alloc, query);
        defer alloc.free(canonical_query);

        const canonical_request = try std.fmt.allocPrint(alloc, "{s}\n{s}\n{s}\n{s}\n{s}\n{s}", .{
            method,
            path,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        });
        defer alloc.free(canonical_request);

        // Step 2: String to Sign
        var canonical_hash: [64]u8 = undefined;
        sha256Hex(canonical_request, &canonical_hash);

        const credential_scope = try std.fmt.allocPrint(alloc, "{s}/{s}/{s}/aws4_request", .{ date_stamp, self.region, self.service });
        defer alloc.free(credential_scope);

        const string_to_sign = try std.fmt.allocPrint(alloc, "AWS4-HMAC-SHA256\n{s}\n{s}\n{s}", .{ amz_date, credential_scope, canonical_hash });
        defer alloc.free(string_to_sign);

        // Step 3: Signing Key
        var k_date: [HmacSha256.mac_length]u8 = undefined;
        var k_region: [HmacSha256.mac_length]u8 = undefined;
        var k_service: [HmacSha256.mac_length]u8 = undefined;
        var k_signing: [HmacSha256.mac_length]u8 = undefined;

        const aws4_key = try std.fmt.allocPrint(alloc, "AWS4{s}", .{self.secret_key});
        defer alloc.free(aws4_key);

        HmacSha256.create(&k_date, date_stamp, aws4_key);
        HmacSha256.create(&k_region, self.region, &k_date);
        HmacSha256.create(&k_service, self.service, &k_region);
        HmacSha256.create(&k_signing, "aws4_request", &k_service);

        // Step 4: Signature
        var sig_raw: [HmacSha256.mac_length]u8 = undefined;
        HmacSha256.create(&sig_raw, string_to_sign, &k_signing);

        const signature = std.fmt.bytesToHex(sig_raw, .lower);

        // Step 5: Authorization Header
        return try std.fmt.allocPrint(alloc, "AWS4-HMAC-SHA256 Credential={s}/{s}, SignedHeaders={s}, Signature={s}", .{
            self.access_key,
            credential_scope,
            signed_headers,
            signature,
        });
    }

    /// Compute SHA-256 hash of data and output as lowercase hex string.
    pub fn sha256Hex(data: []const u8, out: *[64]u8) void {
        var hash: [Sha256.digest_length]u8 = undefined;
        Sha256.hash(data, &hash, .{});
        out.* = std.fmt.bytesToHex(hash, .lower);
    }

    /// Hash of empty payload.
    pub const EMPTY_PAYLOAD_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    const QueryParam = struct {
        name: []const u8,
        value: []const u8,
    };

    fn canonicalQueryString(alloc: Allocator, query: []const u8) ![]u8 {
        if (query.len == 0) return try alloc.dupe(u8, "");

        var params = std.array_list.Managed(QueryParam).init(alloc);
        defer params.deinit();

        var iter = std.mem.splitScalar(u8, query, '&');
        while (iter.next()) |part| {
            if (part.len == 0) continue;
            if (std.mem.indexOfScalar(u8, part, '=')) |eq| {
                try params.append(.{ .name = part[0..eq], .value = part[eq + 1 ..] });
            } else {
                try params.append(.{ .name = part, .value = "" });
            }
        }

        std.mem.sort(QueryParam, params.items, {}, struct {
            fn lessThan(_: void, a: QueryParam, b: QueryParam) bool {
                const name_order = std.mem.order(u8, a.name, b.name);
                if (name_order == .lt) return true;
                if (name_order == .gt) return false;
                return std.mem.lessThan(u8, a.value, b.value);
            }
        }.lessThan);

        var out = std.array_list.Managed(u8).init(alloc);
        errdefer out.deinit();
        for (params.items, 0..) |param, i| {
            if (i > 0) try out.append('&');
            try out.appendSlice(param.name);
            try out.append('=');
            try out.appendSlice(param.value);
        }
        return try out.toOwnedSlice();
    }

    /// Get current date/time in ISO 8601 format for AWS.
    pub fn currentDateTime(date_buf: *[8]u8, datetime_buf: *[16]u8) void {
        const epoch_seconds = @as(u64, @intCast(@divTrunc(@import("time_compat").milliTimestamp(), 1000)));
        const epoch = std.time.epoch.EpochSeconds{ .secs = epoch_seconds };
        const day = epoch.getDaySeconds();
        const year_day = epoch.getEpochDay().calculateYearDay();
        const month_day = year_day.calculateMonthDay();

        _ = std.fmt.bufPrint(date_buf, "{d:0>4}{d:0>2}{d:0>2}", .{ year_day.year, month_day.month.numeric(), month_day.day_index + 1 }) catch {};
        _ = std.fmt.bufPrint(datetime_buf, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{ year_day.year, month_day.month.numeric(), month_day.day_index + 1, day.getHoursIntoDay(), day.getMinutesIntoHour(), day.getSecondsIntoMinute() }) catch {};
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "SHA-256 hex of empty string" {
    var out: [64]u8 = undefined;
    AwsSigV4.sha256Hex("", &out);
    try testing.expectEqualStrings("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", &out);
}

test "SHA-256 hex of 'hello'" {
    var out: [64]u8 = undefined;
    AwsSigV4.sha256Hex("hello", &out);
    try testing.expectEqualStrings("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", &out);
}

test "SigV4 signing produces valid format" {
    const signer = AwsSigV4.init("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    const auth = try signer.sign(
        testing.allocator,
        "GET",
        "/",
        "",
        "examplebucket.s3.amazonaws.com",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20130524",
        "20130524T000000Z",
    );
    defer testing.allocator.free(auth);

    // Verify it starts with the right prefix
    try testing.expect(std.mem.startsWith(u8, auth, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"));
    try testing.expect(std.mem.indexOf(u8, auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") != null);
    try testing.expect(std.mem.indexOf(u8, auth, "Signature=") != null);
}

test "currentDateTime format" {
    var date_buf: [8]u8 = undefined;
    var datetime_buf: [16]u8 = undefined;
    AwsSigV4.currentDateTime(&date_buf, &datetime_buf);

    // Date should be 8 digits
    try testing.expectEqual(@as(usize, 8), date_buf.len);
    // DateTime should be 16 chars ending with Z
    try testing.expectEqual(@as(u8, 'Z'), datetime_buf[15]);
    try testing.expectEqual(@as(u8, 'T'), datetime_buf[8]);
}

test "SigV4 signing deterministic" {
    const signer = AwsSigV4.init("AKID", "SECRET");

    const auth1 = try signer.sign(
        testing.allocator,
        "GET",
        "/bucket/key",
        "",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth1);

    const auth2 = try signer.sign(
        testing.allocator,
        "GET",
        "/bucket/key",
        "",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth2);

    // Same inputs should produce same signature
    try testing.expectEqualStrings(auth1, auth2);
}

test "SigV4 signing honors configured region" {
    const signer = AwsSigV4.initWithRegion("AKID", "SECRET", "us-west-2");

    const auth = try signer.sign(
        testing.allocator,
        "GET",
        "/bucket/key",
        "",
        "bucket.s3.us-west-2.amazonaws.com",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth);

    try testing.expect(std.mem.indexOf(u8, auth, "/20260101/us-west-2/s3/aws4_request") != null);
}

test "SigV4 different methods produce different signatures" {
    const signer = AwsSigV4.init("AKID", "SECRET");

    const auth_get = try signer.sign(
        testing.allocator,
        "GET",
        "/bucket/key",
        "",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth_get);

    const auth_put = try signer.sign(
        testing.allocator,
        "PUT",
        "/bucket/key",
        "",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth_put);

    // GET and PUT should produce different signatures
    try testing.expect(!std.mem.eql(u8, auth_get, auth_put));
}

test "SigV4 with payload hash" {
    const signer = AwsSigV4.init("AKID", "SECRET");

    var payload_hash: [64]u8 = undefined;
    AwsSigV4.sha256Hex("request body data", &payload_hash);

    const auth = try signer.sign(
        testing.allocator,
        "PUT",
        "/bucket/key",
        "",
        "localhost:9000",
        &payload_hash,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth);

    try testing.expect(std.mem.startsWith(u8, auth, "AWS4-HMAC-SHA256 Credential=AKID/"));
}

test "SigV4 with query string" {
    const signer = AwsSigV4.init("AKID", "SECRET");

    const auth = try signer.sign(
        testing.allocator,
        "GET",
        "/bucket",
        "list-type=2&prefix=data/",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth);

    try testing.expect(std.mem.indexOf(u8, auth, "Signature=") != null);
}

test "SigV4 canonicalizes query parameters before signing" {
    const signer = AwsSigV4.init("AKID", "SECRET");

    const auth_unsorted = try signer.sign(
        testing.allocator,
        "GET",
        "/bucket",
        "list-type=2&prefix=wal%2F&continuation-token=next-token",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth_unsorted);

    const auth_sorted = try signer.sign(
        testing.allocator,
        "GET",
        "/bucket",
        "continuation-token=next-token&list-type=2&prefix=wal%2F",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth_sorted);

    try testing.expectEqualStrings(auth_sorted, auth_unsorted);

    const auth_uploads_bare = try signer.sign(
        testing.allocator,
        "POST",
        "/bucket/key",
        "uploads",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth_uploads_bare);

    const auth_uploads_empty = try signer.sign(
        testing.allocator,
        "POST",
        "/bucket/key",
        "uploads=",
        "localhost:9000",
        AwsSigV4.EMPTY_PAYLOAD_HASH,
        "20260101",
        "20260101T000000Z",
    );
    defer testing.allocator.free(auth_uploads_empty);

    try testing.expectEqualStrings(auth_uploads_empty, auth_uploads_bare);
}

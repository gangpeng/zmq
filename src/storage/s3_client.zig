const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const net = std.net;
const log = std.log.scoped(.s3);
const AwsSigV4 = @import("aws_sigv4.zig").AwsSigV4;

/// S3-compatible HTTP client for MinIO.
///
/// Uses raw TCP + HTTP/1.1 to talk to MinIO's S3 API.
/// Supports: PutObject, GetObject, DeleteObject, HeadObject, CreateBucket, ListObjects.
/// Authentication: AWS Signature V4 (compatible with AWS S3 and MinIO).
pub const S3Client = struct {
    host: []const u8,
    port: u16,
    bucket: []const u8,
    access_key: []const u8,
    secret_key: []const u8,
    signer: AwsSigV4,
    allocator: Allocator,
    put_count: u64 = 0,
    get_count: u64 = 0,
    delete_count: u64 = 0,
    bytes_uploaded: u64 = 0,
    bytes_downloaded: u64 = 0,
    // Connection pool for keep-alive reuse
    // TODO: For HTTPS support, add OpenSSL FFI or use Zig's std.crypto.tls.Client
    //       (requires linking against libssl/libcrypto). HTTPS connections would wrap
    //       the raw socket with TLS before HTTP I/O.
    pooled_socket: ?std.posix.socket_t = null,

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 9000,
        bucket: []const u8 = "automq",
        access_key: []const u8 = "minioadmin",
        secret_key: []const u8 = "minioadmin",
    };

    pub fn init(alloc: Allocator, config: Config) S3Client {
        return .{
            .host = config.host,
            .port = config.port,
            .bucket = config.bucket,
            .access_key = config.access_key,
            .secret_key = config.secret_key,
            .signer = AwsSigV4.init(config.access_key, config.secret_key),
            .allocator = alloc,
        };
    }

    /// Ensure the bucket exists.
    pub fn ensureBucket(self: *S3Client) !void {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}", .{self.bucket});
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        _ = try self.httpRequest("PUT", path, "", null, null, &resp_buf);
    }

    /// Upload an object.
    pub fn putObject(self: *S3Client, key: []const u8, data: []const u8) !void {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        const status = try self.httpRequest("PUT", path, "", data, null, &resp_buf);

        if (status < 200 or status >= 300) {
            return error.S3PutFailed;
        }

        self.put_count += 1;
        self.bytes_uploaded += data.len;
    }

    /// Download an object. Caller owns returned slice.
    pub fn getObject(self: *S3Client, key: []const u8) ![]u8 {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        // Use a dynamic buffer for response
        const sock = try self.connect();
        defer std.posix.close(sock);

        // Build signed request
        var date_buf: [8]u8 = undefined;
        var datetime_buf: [16]u8 = undefined;
        AwsSigV4.currentDateTime(&date_buf, &datetime_buf);

        const host_header = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ self.host, self.port });
        defer self.allocator.free(host_header);

        const auth = try self.signer.sign(
            self.allocator,
            "GET",
            path,
            "",
            host_header,
            AwsSigV4.EMPTY_PAYLOAD_HASH,
            &date_buf,
            &datetime_buf,
        );
        defer self.allocator.free(auth);

        const req = try std.fmt.allocPrint(self.allocator, "GET {s} HTTP/1.1\r\nHost: {s}\r\nAuthorization: {s}\r\nx-amz-date: {s}\r\nx-amz-content-sha256: {s}\r\nConnection: close\r\n\r\n", .{ path, host_header, auth, datetime_buf, AwsSigV4.EMPTY_PAYLOAD_HASH });
        defer self.allocator.free(req);

        _ = try std.posix.write(sock, req);

        // Read full response
        var response = std.ArrayList(u8).init(self.allocator);
        defer response.deinit();

        var read_buf: [8192]u8 = undefined;
        while (true) {
            const n = std.posix.read(sock, &read_buf) catch break;
            if (n == 0) break;
            try response.appendSlice(read_buf[0..n]);
        }

        // Parse HTTP response — find \r\n\r\n header/body separator
        const resp_data = response.items;
        const header_end = std.mem.indexOf(u8, resp_data, "\r\n\r\n") orelse return error.S3GetFailed;

        // Check status
        if (resp_data.len < 12) return error.S3GetFailed;
        const status_code = std.fmt.parseInt(u16, resp_data[9..12], 10) catch return error.S3GetFailed;
        if (status_code < 200 or status_code >= 300) return error.S3GetFailed;

        // Extract body
        const body_start = header_end + 4;
        const body = resp_data[body_start..];

        // Check for chunked transfer encoding
        const headers_str = resp_data[0..header_end];
        if (std.mem.indexOf(u8, headers_str, "Transfer-Encoding: chunked") != null) {
            return try self.decodeChunked(body);
        }

        const result = try self.allocator.dupe(u8, body);
        self.get_count += 1;
        self.bytes_downloaded += result.len;
        return result;
    }

    /// List objects in the bucket with optional prefix. Returns XML response body.
    pub fn listObjects(self: *S3Client, prefix: ?[]const u8) ![]u8 {
        const query = if (prefix) |p|
            try std.fmt.allocPrint(self.allocator, "list-type=2&prefix={s}", .{p})
        else
            try self.allocator.dupe(u8, "list-type=2");
        defer self.allocator.free(query);

        const path = try std.fmt.allocPrint(self.allocator, "/{s}", .{self.bucket});
        defer self.allocator.free(path);

        const full_path = try std.fmt.allocPrint(self.allocator, "{s}?{s}", .{ path, query });
        defer self.allocator.free(full_path);

        const sock = try self.connect();
        defer std.posix.close(sock);

        var date_buf: [8]u8 = undefined;
        var datetime_buf: [16]u8 = undefined;
        AwsSigV4.currentDateTime(&date_buf, &datetime_buf);

        const host_header = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ self.host, self.port });
        defer self.allocator.free(host_header);

        const auth = try self.signer.sign(
            self.allocator,
            "GET",
            path,
            query,
            host_header,
            AwsSigV4.EMPTY_PAYLOAD_HASH,
            &date_buf,
            &datetime_buf,
        );
        defer self.allocator.free(auth);

        const req = try std.fmt.allocPrint(self.allocator, "GET {s} HTTP/1.1\r\nHost: {s}\r\nAuthorization: {s}\r\nx-amz-date: {s}\r\nx-amz-content-sha256: {s}\r\nConnection: close\r\n\r\n", .{ full_path, host_header, auth, datetime_buf, AwsSigV4.EMPTY_PAYLOAD_HASH });
        defer self.allocator.free(req);

        _ = try std.posix.write(sock, req);

        var response = std.ArrayList(u8).init(self.allocator);
        defer response.deinit();

        var read_buf: [8192]u8 = undefined;
        while (true) {
            const n = std.posix.read(sock, &read_buf) catch break;
            if (n == 0) break;
            try response.appendSlice(read_buf[0..n]);
        }

        const resp_data = response.items;
        const header_end = std.mem.indexOf(u8, resp_data, "\r\n\r\n") orelse return error.S3ListFailed;
        if (resp_data.len < 12) return error.S3ListFailed;
        const status_code = std.fmt.parseInt(u16, resp_data[9..12], 10) catch return error.S3ListFailed;
        if (status_code < 200 or status_code >= 300) return error.S3ListFailed;

        const body_start = header_end + 4;
        const body = resp_data[body_start..];

        const headers_str = resp_data[0..header_end];
        if (std.mem.indexOf(u8, headers_str, "Transfer-Encoding: chunked") != null) {
            return try self.decodeChunked(body);
        }

        return try self.allocator.dupe(u8, body);
    }

    fn decodeChunked(self: *S3Client, data: []const u8) ![]u8 {
        var result = std.ArrayList(u8).init(self.allocator);
        var pos: usize = 0;

        while (pos < data.len) {
            const line_end = std.mem.indexOf(u8, data[pos..], "\r\n") orelse break;
            const chunk_size_str = data[pos .. pos + line_end];
            const chunk_size = std.fmt.parseInt(usize, chunk_size_str, 16) catch break;

            if (chunk_size == 0) break;

            pos += line_end + 2;
            if (pos + chunk_size > data.len) break;

            try result.appendSlice(data[pos .. pos + chunk_size]);
            pos += chunk_size + 2;
        }

        self.get_count += 1;
        self.bytes_downloaded += result.items.len;
        return result.toOwnedSlice();
    }

    /// Delete an object.
    pub fn deleteObject(self: *S3Client, key: []const u8) !void {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        _ = try self.httpRequest("DELETE", path, "", null, null, &resp_buf);
        self.delete_count += 1;
    }

    /// Check if an object exists.
    pub fn headObject(self: *S3Client, key: []const u8) !bool {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        const status = try self.httpRequest("HEAD", path, "", null, null, &resp_buf);
        return status >= 200 and status < 300;
    }

    /// Read a range of bytes from an S3 object.
    /// Returns the requested byte range, or error if the object doesn't exist.
    pub fn getObjectRange(self: *S3Client, key: []const u8, offset: u64, length: u64) ![]u8 {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        const range_header = try std.fmt.allocPrint(self.allocator, "bytes={d}-{d}", .{ offset, offset + length - 1 });
        defer self.allocator.free(range_header);

        const sock = try self.connect();
        defer std.posix.close(sock);

        var date_buf: [8]u8 = undefined;
        var datetime_buf: [16]u8 = undefined;
        AwsSigV4.currentDateTime(&date_buf, &datetime_buf);

        const host_header = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ self.host, self.port });
        defer self.allocator.free(host_header);

        const auth = try self.signer.sign(self.allocator, "GET", path, "", host_header, AwsSigV4.EMPTY_PAYLOAD_HASH, &date_buf, &datetime_buf);
        defer self.allocator.free(auth);

        const req = try std.fmt.allocPrint(self.allocator, "GET {s} HTTP/1.1\r\nHost: {s}\r\nAuthorization: {s}\r\nx-amz-date: {s}\r\nx-amz-content-sha256: {s}\r\nRange: {s}\r\nConnection: close\r\n\r\n", .{ path, host_header, auth, datetime_buf, AwsSigV4.EMPTY_PAYLOAD_HASH, range_header });
        defer self.allocator.free(req);

        _ = try std.posix.write(sock, req);

        var response = std.ArrayList(u8).init(self.allocator);
        defer response.deinit();

        var read_buf: [8192]u8 = undefined;
        while (true) {
            const n = std.posix.read(sock, &read_buf) catch break;
            if (n == 0) break;
            try response.appendSlice(read_buf[0..n]);
        }

        const resp_data = response.items;
        const header_end = std.mem.indexOf(u8, resp_data, "\r\n\r\n") orelse return error.S3GetFailed;

        if (resp_data.len < 12) return error.S3GetFailed;
        const status_code = std.fmt.parseInt(u16, resp_data[9..12], 10) catch return error.S3GetFailed;
        // 206 = Partial Content, 200 = Full content
        if (status_code != 206 and status_code != 200) return error.S3GetFailed;

        const body_start = header_end + 4;
        const body = resp_data[body_start..];

        const result = try self.allocator.dupe(u8, body);
        self.get_count += 1;
        self.bytes_downloaded += result.len;
        return result;
    }

    /// Upload a large object using S3 multipart upload.
    /// Automatically splits data into 5MB parts.
    pub fn putObjectMultipart(self: *S3Client, key: []const u8, data: []const u8) !void {
        const PART_SIZE: usize = 5 * 1024 * 1024; // 5MB minimum part size

        // If data is small enough, use regular PutObject
        if (data.len <= PART_SIZE) {
            return self.putObject(key, data);
        }

        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        // Step 1: Initiate multipart upload
        const init_path = try std.fmt.allocPrint(self.allocator, "{s}?uploads", .{path});
        defer self.allocator.free(init_path);

        var resp_buf: [4096]u8 = undefined;
        const init_status = try self.httpRequest("POST", init_path, "uploads", null, null, &resp_buf);
        if (init_status < 200 or init_status >= 300) return error.S3MultipartInitFailed;

        // Parse UploadId from XML response (simplified)
        const upload_id = blk: {
            const resp_str = resp_buf[0..];
            const id_start = std.mem.indexOf(u8, resp_str, "<UploadId>") orelse return error.S3MultipartInitFailed;
            const id_end = std.mem.indexOf(u8, resp_str[id_start..], "</UploadId>") orelse return error.S3MultipartInitFailed;
            break :blk resp_str[id_start + 10 .. id_start + id_end];
        };

        // Step 2: Upload parts
        var part_number: u32 = 1;
        var offset: usize = 0;
        var etags: [2048]struct { part: u32, etag: []const u8 } = undefined;
        var num_parts: usize = 0;

        while (offset < data.len) {
            const end = @min(offset + PART_SIZE, data.len);
            const part_data = data[offset..end];

            const part_path = try std.fmt.allocPrint(self.allocator, "{s}?partNumber={d}&uploadId={s}", .{ path, part_number, upload_id });
            defer self.allocator.free(part_path);

            const query = try std.fmt.allocPrint(self.allocator, "partNumber={d}&uploadId={s}", .{ part_number, upload_id });
            defer self.allocator.free(query);

            var part_resp: [4096]u8 = undefined;
            const part_status = try self.httpRequest("PUT", path, query, part_data, null, &part_resp);
            if (part_status < 200 or part_status >= 300) return error.S3PartUploadFailed;

            // Parse actual ETag from response headers
            const etag_str = blk: {
                const resp_str = part_resp[0..];
                if (std.mem.indexOf(u8, resp_str, "ETag: ")) |etag_start| {
                    const etag_value_start = etag_start + 6;
                    if (std.mem.indexOf(u8, resp_str[etag_value_start..], "\r\n")) |etag_end| {
                        break :blk resp_str[etag_value_start .. etag_value_start + etag_end];
                    }
                }
                // Fallback: use part number as placeholder
                break :blk "";
            };
            etags[num_parts] = .{ .part = part_number, .etag = etag_str };
            num_parts += 1;

            offset = end;
            part_number += 1;
        }

        // Step 3: Complete multipart upload
        // Build XML body with part ETags
        var complete_body = std.ArrayList(u8).init(self.allocator);
        defer complete_body.deinit();
        try complete_body.appendSlice("<CompleteMultipartUpload>");
        for (etags[0..num_parts]) |etag| {
            const part_xml = try std.fmt.allocPrint(self.allocator, "<Part><PartNumber>{d}</PartNumber><ETag>\"{s}\"</ETag></Part>", .{ etag.part, etag.etag });
            defer self.allocator.free(part_xml);
            try complete_body.appendSlice(part_xml);
        }
        try complete_body.appendSlice("</CompleteMultipartUpload>");

        const complete_query = try std.fmt.allocPrint(self.allocator, "uploadId={s}", .{upload_id});
        defer self.allocator.free(complete_query);

        var complete_resp: [4096]u8 = undefined;
        const complete_status = try self.httpRequest("POST", path, complete_query, complete_body.items, null, &complete_resp);
        if (complete_status < 200 or complete_status >= 300) return error.S3MultipartCompleteFailed;

        self.put_count += 1;
        self.bytes_uploaded += data.len;

        log.info("Multipart upload completed: {s} ({d} parts, {d} bytes)", .{ key, num_parts, data.len });
    }

    /// Connection with reuse support (keep-alive pooling).
    /// For simple requests (httpRequestOnce), we still use Connection: close.
    /// A full connection pool would maintain multiple sockets per host.
    fn connect(self: *const S3Client) !std.posix.socket_t {
        // Check for pooled socket (basic single-connection pool)
        if (self.pooled_socket) |sock| {
            // Verify socket is still valid by attempting a zero-byte read
            var test_buf: [1]u8 = undefined;
            const result = std.posix.recv(sock, &test_buf, std.posix.MSG.PEEK | std.posix.MSG.DONTWAIT) catch {
                // Socket is dead, create new one
                std.posix.close(sock);
                const mutable_self = @constCast(self);
                mutable_self.pooled_socket = null;
                return connectNew(self);
            };
            _ = result;
            return sock;
        }
        return connectNew(self);
    }

    fn connectNew(self: *const S3Client) !std.posix.socket_t {
        // Try numeric IP first, fall back to DNS resolution for hostnames (e.g. "minio" in Docker)
        const address = net.Address.parseIp4(self.host, self.port) catch blk: {
            // Hostname — resolve via system DNS (getaddrinfo)
            const addr_list = try net.getAddressList(self.allocator, self.host, self.port);
            defer addr_list.deinit();
            if (addr_list.addrs.len == 0) return error.UnknownHostName;
            break :blk addr_list.addrs[0];
        };
        const sock = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer std.posix.close(sock);
        try std.posix.connect(sock, &address.any, address.getOsSockLen());
        return sock;
    }

    fn httpRequest(self: *S3Client, method: []const u8, path: []const u8, query: []const u8, body: ?[]const u8, range_header: ?[]const u8, resp_buf: []u8) !u16 {
        // Retry with exponential backoff
        const MAX_RETRIES: u32 = 3;
        var attempt: u32 = 0;
        while (true) {
            const result = self.httpRequestOnce(method, path, query, body, range_header, resp_buf);
            if (result) |status| {
                return status;
            } else |err| {
                attempt += 1;
                if (attempt >= MAX_RETRIES) {
                    log.warn("S3 request failed after {d} retries: {s} {s}", .{ MAX_RETRIES, method, path });
                    return err;
                }
                // Exponential backoff: 100ms, 200ms, 400ms...
                const delay_ms: u64 = @as(u64, 100) << @intCast(attempt - 1);
                log.debug("S3 request retry {d}/{d} in {d}ms", .{ attempt, MAX_RETRIES, delay_ms });
                std.time.sleep(delay_ms * std.time.ns_per_ms);
            }
        }
    }

    fn httpRequestOnce(self: *S3Client, method: []const u8, path: []const u8, query: []const u8, body: ?[]const u8, range_header: ?[]const u8, resp_buf: []u8) !u16 {
        const sock = try self.connect();
        defer std.posix.close(sock);

        // Get current datetime for signing
        var date_buf: [8]u8 = undefined;
        var datetime_buf: [16]u8 = undefined;
        AwsSigV4.currentDateTime(&date_buf, &datetime_buf);

        // Compute payload hash
        var payload_hash: [64]u8 = undefined;
        if (body) |b| {
            AwsSigV4.sha256Hex(b, &payload_hash);
        } else {
            @memcpy(&payload_hash, AwsSigV4.EMPTY_PAYLOAD_HASH);
        }

        const host_header = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ self.host, self.port });
        defer self.allocator.free(host_header);

        // Sign the request
        const auth = try self.signer.sign(
            self.allocator,
            method,
            path,
            query,
            host_header,
            &payload_hash,
            &date_buf,
            &datetime_buf,
        );
        defer self.allocator.free(auth);

        // Build request
        var req_buf = std.ArrayList(u8).init(self.allocator);
        defer req_buf.deinit();
        const writer = req_buf.writer();

        try writer.print("{s} {s} HTTP/1.1\r\n", .{ method, path });
        try writer.print("Host: {s}\r\n", .{host_header});
        try writer.print("Authorization: {s}\r\n", .{auth});
        try writer.print("x-amz-date: {s}\r\n", .{datetime_buf});
        try writer.print("x-amz-content-sha256: {s}\r\n", .{payload_hash});

        if (body) |b| {
            try writer.print("Content-Length: {d}\r\n", .{b.len});
        }
        if (range_header) |rh| {
            try writer.print("Range: {s}\r\n", .{rh});
        }
        try writer.print("Connection: close\r\n\r\n", .{});

        _ = try std.posix.write(sock, req_buf.items);
        if (body) |b| {
            _ = try std.posix.write(sock, b);
        }

        // Read response
        var total_read: usize = 0;
        while (total_read < resp_buf.len) {
            const n = std.posix.read(sock, resp_buf[total_read..]) catch break;
            if (n == 0) break;
            total_read += n;
            if (std.mem.indexOf(u8, resp_buf[0..total_read], "\r\n") != null) break;
        }

        if (total_read < 12) return error.S3RequestFailed;

        const status = std.fmt.parseInt(u16, resp_buf[9..12], 10) catch return error.S3RequestFailed;
        return status;
    }
};

/// Unified S3 storage interface — works with either MockS3 or real S3Client.
pub const S3Storage = struct {
    mock: ?*MockS3,
    client: ?*S3Client,
    allocator: Allocator,

    const MockS3 = @import("s3.zig").MockS3;

    pub fn initMock(alloc: Allocator, mock: *MockS3) S3Storage {
        return .{ .mock = mock, .client = null, .allocator = alloc };
    }

    pub fn initReal(alloc: Allocator, client: *S3Client) S3Storage {
        return .{ .mock = null, .client = client, .allocator = alloc };
    }

    pub fn putObject(self: *S3Storage, key: []const u8, data: []const u8) !void {
        if (self.client) |c| {
            if (data.len > 5 * 1024 * 1024) {
                try c.putObjectMultipart(key, data);
            } else {
                try c.putObject(key, data);
            }
        } else if (self.mock) |m| {
            try m.putObject(key, data);
        }
    }

    pub fn getObject(self: *S3Storage, key: []const u8) !?[]u8 {
        if (self.client) |c| {
            return c.getObject(key) catch |err| {
                log.warn("S3 GetObject failed for key '{s}': {}", .{ key, err });
                return null;
            };
        } else if (self.mock) |m| {
            if (m.getObject(key)) |data| {
                return try self.allocator.dupe(u8, data);
            }
        }
        return null;
    }

    pub fn deleteObject(self: *S3Storage, key: []const u8) !void {
        if (self.client) |c| {
            try c.deleteObject(key);
        } else if (self.mock) |m| {
            _ = m.deleteObject(key);
        }
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "S3Client init" {
    const client = S3Client.init(testing.allocator, .{});
    try testing.expectEqualStrings("127.0.0.1", client.host);
    try testing.expectEqual(@as(u16, 9000), client.port);
    try testing.expectEqual(@as(u64, 0), client.put_count);
}

test "S3Storage mock mode" {
    var mock = @import("s3.zig").MockS3.init(testing.allocator);
    defer mock.deinit();

    var storage = S3Storage.initMock(testing.allocator, &mock);

    try storage.putObject("key1", "hello");
    const data = try storage.getObject("key1");
    if (data) |d| {
        defer testing.allocator.free(d);
        try testing.expectEqualStrings("hello", d);
    } else {
        return error.TestUnexpectedResult;
    }

    try storage.deleteObject("key1");
    const data2 = try storage.getObject("key1");
    try testing.expect(data2 == null);
}

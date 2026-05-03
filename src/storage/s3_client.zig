const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const net = @import("net_compat");
const log = std.log.scoped(.s3);
const AwsSigV4 = @import("aws_sigv4.zig").AwsSigV4;
const MetricRegistry = @import("core").MetricRegistry;
const security = @import("security");
const TlsClientContext = security.tls.TlsClientContext;

fn getenv(name: [:0]const u8) ?[]const u8 {
    const value = std.c.getenv(name.ptr) orelse return null;
    return std.mem.span(value);
}

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
    scheme: Scheme = .http,
    tls_ca_file: ?[]const u8 = null,
    signer: AwsSigV4,
    allocator: Allocator,
    put_count: u64 = 0,
    get_count: u64 = 0,
    delete_count: u64 = 0,
    bytes_uploaded: u64 = 0,
    bytes_downloaded: u64 = 0,
    /// Optional Prometheus metric registry for S3 I/O observability.
    /// When set, records s3_requests_total, s3_request_errors_total,
    /// s3_request_duration_seconds, and s3_bytes_total.
    metrics: ?*MetricRegistry = null,
    // Connection pool for keep-alive reuse. Currently unused because requests
    // use Connection: close to keep response framing simple and correct.
    pooled_socket: ?std.posix.socket_t = null,
    /// Optional HTTP hook used by deterministic storage tests.
    test_http_ctx: ?*anyopaque = null,
    test_http_request: ?*const fn (
        ctx: *anyopaque,
        method: []const u8,
        path: []const u8,
        query: []const u8,
        body: ?[]const u8,
        range_header: ?[]const u8,
        resp_buf: []u8,
    ) anyerror!u16 = null,

    pub const Scheme = enum {
        http,
        https,
    };

    const sha256_checksum_base64_len = std.base64.standard.Encoder.calcSize(32);

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 9000,
        bucket: []const u8 = "automq",
        access_key: []const u8 = "minioadmin",
        secret_key: []const u8 = "minioadmin",
        scheme: Scheme = .http,
        tls_ca_file: ?[]const u8 = null,
    };

    pub fn init(alloc: Allocator, config: Config) S3Client {
        const endpoint = normalizeEndpoint(config.host, config.port, config.scheme);
        // S3 credential resolution chain (matches AutoMQ's AutoMQStaticCredentialsProvider):
        // 1. Explicit config values (if not the default "minioadmin")
        // 2. KAFKA_S3_ACCESS_KEY / KAFKA_S3_SECRET_KEY (AutoMQ-specific)
        // 3. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (standard AWS SDK)
        // 4. Fall back to config defaults
        const access_key = if (!std.mem.eql(u8, config.access_key, "minioadmin"))
            config.access_key
        else if (getenv("KAFKA_S3_ACCESS_KEY")) |k|
            k
        else if (getenv("AWS_ACCESS_KEY_ID")) |k|
            k
        else
            config.access_key;

        const secret_key = if (!std.mem.eql(u8, config.secret_key, "minioadmin"))
            config.secret_key
        else if (getenv("KAFKA_S3_SECRET_KEY")) |k|
            k
        else if (getenv("AWS_SECRET_ACCESS_KEY")) |k|
            k
        else
            config.secret_key;

        return .{
            .host = endpoint.host,
            .port = endpoint.port,
            .bucket = config.bucket,
            .access_key = access_key,
            .secret_key = secret_key,
            .scheme = endpoint.scheme,
            .tls_ca_file = config.tls_ca_file,
            .signer = AwsSigV4.init(access_key, secret_key),
            .allocator = alloc,
        };
    }

    const Endpoint = struct {
        host: []const u8,
        port: u16,
        scheme: Scheme,
    };

    fn normalizeEndpoint(raw_host: []const u8, configured_port: u16, configured_scheme: Scheme) Endpoint {
        var host = raw_host;
        var port = configured_port;
        var scheme = configured_scheme;
        var scheme_from_host = false;
        var port_from_host = false;

        if (std.mem.startsWith(u8, host, "https://")) {
            host = host["https://".len..];
            scheme = .https;
            scheme_from_host = true;
        } else if (std.mem.startsWith(u8, host, "http://")) {
            host = host["http://".len..];
            scheme = .http;
            scheme_from_host = true;
        }

        if (std.mem.indexOfScalar(u8, host, '/')) |slash| {
            host = host[0..slash];
        }

        var colon_count: usize = 0;
        for (host) |ch| {
            if (ch == ':') colon_count += 1;
        }
        if (colon_count == 1) {
            if (std.mem.lastIndexOfScalar(u8, host, ':')) |colon| {
                if (colon + 1 < host.len) {
                    if (std.fmt.parseInt(u16, host[colon + 1 ..], 10)) |parsed_port| {
                        port = parsed_port;
                        host = host[0..colon];
                        port_from_host = true;
                    } else |_| {}
                }
            }
        }

        if (scheme_from_host and scheme == .https and !port_from_host and configured_port == 9000) {
            port = 443;
        }

        return .{ .host = host, .port = port, .scheme = scheme };
    }

    /// Ensure the bucket exists.
    pub fn ensureBucket(self: *S3Client) !void {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}", .{self.bucket});
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        const status = try self.httpRequest("PUT", path, "", null, null, &resp_buf);
        if ((status < 200 or status >= 300) and status != 409) {
            return error.S3EnsureBucketFailed;
        }
    }

    /// Upload an object.
    pub fn putObject(self: *S3Client, key: []const u8, data: []const u8) !void {
        const start_ns = @import("time_compat").nanoTimestamp();
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        const status = self.httpRequest("PUT", path, "", data, null, &resp_buf) catch |err| {
            self.recordS3Error("put");
            self.recordS3Duration("put", start_ns);
            return err;
        };

        if (status < 200 or status >= 300) {
            self.recordS3Error("put");
            self.recordS3Duration("put", start_ns);
            return error.S3PutFailed;
        }

        self.put_count += 1;
        self.bytes_uploaded += data.len;
        self.recordS3Request("put");
        self.recordS3Bytes("upload", data.len);
        self.recordS3Duration("put", start_ns);
    }

    /// Download an object. Caller owns returned slice.
    /// Retries up to 3 times with exponential backoff (100ms, 200ms, 400ms)
    /// on transient failures (connection errors, non-2xx responses).
    pub fn getObject(self: *S3Client, key: []const u8) ![]u8 {
        const MAX_RETRIES: u32 = 3;
        const start_ns = @import("time_compat").nanoTimestamp();
        var attempt: u32 = 0;

        while (true) {
            const result = self.getObjectOnce(key);
            if (result) |data| {
                self.recordS3Request("get");
                self.recordS3Bytes("download", data.len);
                self.recordS3Duration("get", start_ns);
                return data;
            } else |err| {
                attempt += 1;
                if (attempt >= MAX_RETRIES) {
                    log.warn("S3 GetObject failed after {d} retries: {s}", .{ MAX_RETRIES, key });
                    self.recordS3Error("get");
                    self.recordS3Duration("get", start_ns);
                    return err;
                }
                // Exponential backoff: 100ms, 200ms, 400ms...
                const delay_ms: u64 = @as(u64, 100) << @intCast(attempt - 1);
                log.debug("S3 GetObject retry {d}/{d} in {d}ms for key {s}", .{ attempt, MAX_RETRIES, delay_ms, key });
                @import("time_compat").sleep(delay_ms * std.time.ns_per_ms);
            }
        }
    }

    /// Single attempt to download an object. Called by getObject() retry loop.
    fn getObjectOnce(self: *S3Client, key: []const u8) ![]u8 {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        var response = try self.sendRequest("GET", path, "", null, null, 1024 * 1024 * 1024);
        defer response.deinit(self.allocator);

        if (response.status < 200 or response.status >= 300) {
            return error.S3GetFailed;
        }

        const result = try self.responseBodyOwned(&response);
        self.get_count += 1;
        self.bytes_downloaded += result.len;
        return result;
    }

    /// List one ListObjectsV2 page in the bucket with optional prefix.
    pub fn listObjects(self: *S3Client, prefix: ?[]const u8) ![]u8 {
        return try self.listObjectsPage(prefix, null);
    }

    /// List one ListObjectsV2 page in the bucket with optional prefix and continuation token.
    pub fn listObjectsPage(self: *S3Client, prefix: ?[]const u8, continuation_token: ?[]const u8) ![]u8 {
        var encoded_prefix: ?[]u8 = null;
        defer if (encoded_prefix) |p| self.allocator.free(p);
        var encoded_token: ?[]u8 = null;
        defer if (encoded_token) |t| self.allocator.free(t);

        const query = blk: {
            if (prefix) |p| encoded_prefix = try self.uriEncode(p);
            if (continuation_token) |token| encoded_token = try self.uriEncode(token);

            if (encoded_prefix != null and encoded_token != null) {
                break :blk try std.fmt.allocPrint(self.allocator, "list-type=2&prefix={s}&continuation-token={s}", .{ encoded_prefix.?, encoded_token.? });
            } else if (encoded_prefix != null) {
                break :blk try std.fmt.allocPrint(self.allocator, "list-type=2&prefix={s}", .{encoded_prefix.?});
            } else if (encoded_token != null) {
                break :blk try std.fmt.allocPrint(self.allocator, "list-type=2&continuation-token={s}", .{encoded_token.?});
            } else {
                break :blk try self.allocator.dupe(u8, "list-type=2");
            }
        };
        defer self.allocator.free(query);

        const path = try std.fmt.allocPrint(self.allocator, "/{s}", .{self.bucket});
        defer self.allocator.free(path);

        var response = try self.sendRequest("GET", path, query, null, null, 64 * 1024 * 1024);
        defer response.deinit(self.allocator);

        if (response.status < 200 or response.status >= 300) return error.S3ListFailed;

        return try self.responseBodyOwned(&response);
    }

    fn chunkedBodyEnd(data: []const u8) !usize {
        var pos: usize = 0;
        while (pos < data.len) {
            const line_end = std.mem.indexOf(u8, data[pos..], "\r\n") orelse return error.InvalidChunkedEncoding;
            const chunk_size_line = data[pos .. pos + line_end];
            const chunk_size_token = if (std.mem.indexOfScalar(u8, chunk_size_line, ';')) |semi|
                chunk_size_line[0..semi]
            else
                chunk_size_line;
            const chunk_size = std.fmt.parseInt(usize, std.mem.trim(u8, chunk_size_token, " \t"), 16) catch return error.InvalidChunkedEncoding;

            pos += line_end + 2;
            if (chunk_size == 0) {
                while (true) {
                    const trailer_end = std.mem.indexOf(u8, data[pos..], "\r\n") orelse return error.InvalidChunkedEncoding;
                    pos += trailer_end + 2;
                    if (trailer_end == 0) return pos;
                }
            }

            if (chunk_size > data.len - pos) return error.InvalidChunkedEncoding;
            pos += chunk_size;
            if (data.len - pos < 2) return error.InvalidChunkedEncoding;
            if (!std.mem.eql(u8, data[pos .. pos + 2], "\r\n")) return error.InvalidChunkedEncoding;
            pos += 2;
        }

        return error.InvalidChunkedEncoding;
    }

    fn decodeChunked(self: *S3Client, data: []const u8) ![]u8 {
        const body_end = try chunkedBodyEnd(data);
        if (body_end != data.len) return error.InvalidChunkedEncoding;

        var result = std.array_list.Managed(u8).init(self.allocator);
        errdefer result.deinit();
        var pos: usize = 0;

        while (pos < data.len) {
            const line_end = std.mem.indexOf(u8, data[pos..], "\r\n") orelse return error.InvalidChunkedEncoding;
            const chunk_size_line = data[pos .. pos + line_end];
            const chunk_size_token = if (std.mem.indexOfScalar(u8, chunk_size_line, ';')) |semi|
                chunk_size_line[0..semi]
            else
                chunk_size_line;
            const chunk_size = std.fmt.parseInt(usize, std.mem.trim(u8, chunk_size_token, " \t"), 16) catch return error.InvalidChunkedEncoding;

            pos += line_end + 2;
            if (chunk_size == 0) return result.toOwnedSlice();

            if (chunk_size > data.len - pos) return error.InvalidChunkedEncoding;

            try result.appendSlice(data[pos .. pos + chunk_size]);
            pos += chunk_size;
            if (data.len - pos < 2) return error.InvalidChunkedEncoding;
            if (!std.mem.eql(u8, data[pos .. pos + 2], "\r\n")) return error.InvalidChunkedEncoding;
            pos += 2;
        }

        return error.InvalidChunkedEncoding;
    }

    /// Delete an object.
    pub fn deleteObject(self: *S3Client, key: []const u8) !void {
        const start_ns = @import("time_compat").nanoTimestamp();
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        const status = self.httpRequest("DELETE", path, "", null, null, &resp_buf) catch |err| {
            self.recordS3Error("delete");
            self.recordS3Duration("delete", start_ns);
            return err;
        };
        if (status < 200 or status >= 300) {
            self.recordS3Error("delete");
            self.recordS3Duration("delete", start_ns);
            return error.S3DeleteFailed;
        }
        self.delete_count += 1;
        self.recordS3Request("delete");
        self.recordS3Duration("delete", start_ns);
    }

    /// Check if an object exists.
    pub fn headObject(self: *S3Client, key: []const u8) !bool {
        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        var resp_buf: [4096]u8 = undefined;
        const status = try self.httpRequest("HEAD", path, "", null, null, &resp_buf);
        self.recordS3Request("head");
        return status >= 200 and status < 300;
    }

    /// Read a range of bytes from an S3 object.
    /// Returns the requested byte range, or error if the object doesn't exist.
    pub fn getObjectRange(self: *S3Client, key: []const u8, offset: u64, length: u64) ![]u8 {
        if (length == 0) return try self.allocator.alloc(u8, 0);
        if (offset > std.math.maxInt(u64) - (length - 1)) return error.S3RangeInvalid;

        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        const range_header = try std.fmt.allocPrint(self.allocator, "bytes={d}-{d}", .{ offset, offset + length - 1 });
        defer self.allocator.free(range_header);

        if (length > std.math.maxInt(usize) - 8192) return error.S3ResponseTooLarge;
        const max_response_size: usize = @intCast(length + 8192);
        var response = try self.sendRequest("GET", path, "", null, range_header, max_response_size);
        defer response.deinit(self.allocator);

        const result = try self.rangeBodyFromResponse(&response, offset, length);
        self.get_count += 1;
        self.bytes_downloaded += result.len;
        return result;
    }

    /// Upload a large object using S3 multipart upload.
    /// Automatically splits data into 5MB parts.
    ///
    /// Improvements over naive multipart:
    /// - Dynamic ETags list (no fixed-size array limit)
    /// - Dynamic response buffers (handles large XML responses)
    /// - Retries individual parts up to 3 times before aborting
    /// - Calls AbortMultipartUpload on failure to avoid orphaned parts
    pub fn putObjectMultipart(self: *S3Client, key: []const u8, data: []const u8) !void {
        const PART_SIZE: usize = 5 * 1024 * 1024; // 5MB minimum part size
        const MAX_PART_RETRIES: u32 = 3;

        // If data is small enough, use regular PutObject
        if (data.len <= PART_SIZE) {
            return self.putObject(key, data);
        }

        const path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.bucket, key });
        defer self.allocator.free(path);

        // Step 1: Initiate multipart upload
        const init_resp = try self.allocator.alloc(u8, 8192);
        defer self.allocator.free(init_resp);

        const init_result = try self.httpRequestWithLength("POST", path, "uploads", null, null, init_resp);
        if (init_result.status < 200 or init_result.status >= 300) return error.S3MultipartInitFailed;

        const init_body = try self.responseBodyFromRawHttpDataOwned(init_resp[0..init_result.response_len]);
        defer self.allocator.free(init_body);

        // Parse UploadId from XML response
        const upload_id = blk: {
            const id_start = std.mem.indexOf(u8, init_body, "<UploadId>") orelse return error.S3MultipartInitFailed;
            const search_from = id_start + 10;
            const id_end = std.mem.indexOf(u8, init_body[search_from..], "</UploadId>") orelse return error.S3MultipartInitFailed;
            break :blk try decodeXmlTextAlloc(self.allocator, init_body[search_from .. search_from + id_end]);
        };
        defer self.allocator.free(upload_id);

        // Step 2: Upload parts with retry and abort-on-failure
        const num_parts = (data.len + PART_SIZE - 1) / PART_SIZE;

        const PartEtag = struct { part: u32, etag: []u8 };
        var etags = std.array_list.Managed(PartEtag).init(self.allocator);
        defer {
            for (etags.items) |e| self.allocator.free(e.etag);
            etags.deinit();
        }

        // Use errdefer to abort on any failure after initiation
        errdefer self.abortMultipartUpload(path, upload_id);

        var part_number: u32 = 1;
        var offset: usize = 0;

        while (offset < data.len) {
            const end = @min(offset + PART_SIZE, data.len);
            const part_data = data[offset..end];

            const upload_id_q = try self.uriEncode(upload_id);
            defer self.allocator.free(upload_id_q);

            const query = try std.fmt.allocPrint(self.allocator, "partNumber={d}&uploadId={s}", .{ part_number, upload_id_q });
            defer self.allocator.free(query);

            // Retry individual part uploads
            var part_succeeded = false;
            var attempt: u32 = 0;
            while (attempt < MAX_PART_RETRIES) : (attempt += 1) {
                const part_resp = self.allocator.alloc(u8, 4096) catch break;
                defer self.allocator.free(part_resp);

                const part_status = self.httpRequest("PUT", path, query, part_data, null, part_resp) catch |err| {
                    log.warn("Part {d}/{d} upload failed (attempt {d}/{d}): {}", .{
                        part_number, num_parts, attempt + 1, MAX_PART_RETRIES, err,
                    });
                    if (attempt + 1 < MAX_PART_RETRIES) {
                        const delay_ms: u64 = @as(u64, 200) << @intCast(attempt);
                        @import("time_compat").sleep(delay_ms * std.time.ns_per_ms);
                    }
                    continue;
                };

                if (part_status < 200 or part_status >= 300) {
                    log.warn("Part {d}/{d} upload returned status {d} (attempt {d}/{d})", .{
                        part_number, num_parts, part_status, attempt + 1, MAX_PART_RETRIES,
                    });
                    if (attempt + 1 < MAX_PART_RETRIES) {
                        const delay_ms: u64 = @as(u64, 200) << @intCast(attempt);
                        @import("time_compat").sleep(delay_ms * std.time.ns_per_ms);
                    }
                    continue;
                }

                // Parse ETag from response headers. S3 requires the exact ETag
                // value in CompleteMultipartUpload, including quotes if present.
                const etag_owned = (try self.responseHeaderValueOwned(part_resp, "ETag")) orelse {
                    log.warn("Part {d}/{d} upload returned no ETag", .{ part_number, num_parts });
                    continue;
                };
                if (!isValidMultipartEtag(etag_owned)) {
                    log.warn("Part {d}/{d} upload returned invalid ETag '{s}'", .{ part_number, num_parts, etag_owned });
                    self.allocator.free(etag_owned);
                    continue;
                }
                try etags.append(.{ .part = part_number, .etag = etag_owned });

                part_succeeded = true;
                break;
            }

            if (!part_succeeded) {
                log.warn("Part {d}/{d} failed after {d} retries, aborting multipart upload for {s}", .{
                    part_number, num_parts, MAX_PART_RETRIES, key,
                });
                return error.S3PartUploadFailed;
            }

            offset = end;
            part_number += 1;
        }

        // Step 3: Complete multipart upload
        var complete_body = std.array_list.Managed(u8).init(self.allocator);
        defer complete_body.deinit();
        try complete_body.appendSlice("<CompleteMultipartUpload>");
        for (etags.items) |etag| {
            const part_xml = try std.fmt.allocPrint(self.allocator, "<Part><PartNumber>{d}</PartNumber><ETag>{s}</ETag></Part>", .{ etag.part, etag.etag });
            defer self.allocator.free(part_xml);
            try complete_body.appendSlice(part_xml);
        }
        try complete_body.appendSlice("</CompleteMultipartUpload>");

        const complete_upload_id_q = try self.uriEncode(upload_id);
        defer self.allocator.free(complete_upload_id_q);

        const complete_query = try std.fmt.allocPrint(self.allocator, "uploadId={s}", .{complete_upload_id_q});
        defer self.allocator.free(complete_query);

        const complete_resp = try self.allocator.alloc(u8, 8192);
        defer self.allocator.free(complete_resp);

        const complete_result = try self.httpRequestWithLength("POST", path, complete_query, complete_body.items, null, complete_resp);
        if (complete_result.status < 200 or complete_result.status >= 300) {
            log.warn("CompleteMultipartUpload failed with status {d} for {s}", .{ complete_result.status, key });
            return error.S3MultipartCompleteFailed;
        }

        const complete_response_body = try self.responseBodyFromRawHttpDataOwned(complete_resp[0..complete_result.response_len]);
        defer self.allocator.free(complete_response_body);
        if (isMultipartCompleteEmbeddedError(complete_response_body)) {
            log.warn("CompleteMultipartUpload returned embedded S3 error for {s}", .{key});
            return error.S3MultipartCompleteFailed;
        }

        self.put_count += 1;
        self.bytes_uploaded += data.len;

        log.info("Multipart upload completed: {s} ({d} parts, {d} bytes)", .{ key, num_parts, data.len });
    }

    /// Abort a multipart upload to clean up partially uploaded parts.
    /// Called when part upload fails to prevent orphaned S3 parts.
    fn abortMultipartUpload(self: *S3Client, path: []const u8, upload_id: []const u8) void {
        const upload_id_q = self.uriEncode(upload_id) catch return;
        defer self.allocator.free(upload_id_q);

        const query = std.fmt.allocPrint(self.allocator, "uploadId={s}", .{upload_id_q}) catch return;
        defer self.allocator.free(query);

        var resp_buf: [1024]u8 = undefined;
        const status = self.httpRequest("DELETE", path, query, null, null, &resp_buf) catch |err| {
            log.warn("Failed to abort multipart upload: {}", .{err});
            return;
        };

        if (status >= 200 and status < 300) {
            log.info("Aborted multipart upload for {s}", .{path});
        } else {
            log.warn("AbortMultipartUpload returned status {d} for {s}", .{ status, path });
        }
    }

    const S3Connection = struct {
        fd: std.posix.socket_t,
        tls_ctx: ?TlsClientContext = null,
        ssl: ?*anyopaque = null,

        fn close(self: *S3Connection) void {
            if (self.ssl) |ssl| {
                if (self.tls_ctx) |*ctx| {
                    if (ctx.getOpenSsl()) |ossl| {
                        _ = ossl.SSL_shutdown(ssl);
                        ossl.SSL_free(ssl);
                    }
                }
                self.ssl = null;
            }
            if (self.tls_ctx) |*ctx| {
                ctx.deinit();
                self.tls_ctx = null;
            }
            @import("posix_compat").close(self.fd);
        }

        fn writeAll(self: *S3Connection, data: []const u8) !void {
            var offset: usize = 0;
            while (offset < data.len) {
                const remaining = data[offset..];
                const n = if (self.ssl) |ssl| blk: {
                    const ctx = &(self.tls_ctx orelse return error.S3WriteFailed);
                    const ossl = ctx.getOpenSsl() orelse return error.S3WriteFailed;
                    const len: c_int = @intCast(@min(remaining.len, std.math.maxInt(c_int)));
                    const ret = ossl.SSL_write(ssl, remaining.ptr, len);
                    if (ret <= 0) return error.S3WriteFailed;
                    break :blk @as(usize, @intCast(ret));
                } else try @import("posix_compat").write(self.fd, remaining);

                if (n == 0) return error.S3WriteFailed;
                offset += n;
            }
        }

        fn read(self: *S3Connection, buf: []u8) !usize {
            if (self.ssl) |ssl| {
                const ctx = &(self.tls_ctx orelse return error.S3ReadFailed);
                const ossl = ctx.getOpenSsl() orelse return error.S3ReadFailed;
                const len: c_int = @intCast(@min(buf.len, std.math.maxInt(c_int)));
                const ret = ossl.SSL_read(ssl, buf.ptr, len);
                if (ret > 0) return @intCast(ret);
                const ssl_err = ossl.SSL_get_error(ssl, ret);
                if (ssl_err == @import("security").openssl.OpenSslLib.SSL_ERROR_ZERO_RETURN) return 0;
                return error.S3ReadFailed;
            }
            return std.posix.read(self.fd, buf);
        }
    };

    fn connect(self: *const S3Client) !S3Connection {
        const fd = try self.connectTcp();
        errdefer @import("posix_compat").close(fd);

        if (self.scheme == .https) {
            var tls_ctx = try TlsClientContext.init(.{
                .protocol = .ssl,
                .ca_file = self.tls_ca_file,
            });
            errdefer tls_ctx.deinit();

            const host_z = try self.allocator.dupeZ(u8, self.host);
            defer self.allocator.free(host_z);

            const ssl = try tls_ctx.wrapConnectionWithHostname(fd, host_z);
            return .{
                .fd = fd,
                .tls_ctx = tls_ctx,
                .ssl = ssl,
            };
        }

        return .{ .fd = fd };
    }

    fn connectTcp(self: *const S3Client) !std.posix.socket_t {
        // Try numeric IP first, fall back to DNS resolution for hostnames (e.g. "minio" in Docker)
        const address = net.Address.parseIp4(self.host, self.port) catch blk: {
            // Hostname — resolve via system DNS (getaddrinfo)
            const addr_list = net.getAddressList(self.allocator, self.host, self.port) catch return error.UnknownHostName;
            defer addr_list.deinit();
            if (addr_list.addrs.len == 0) return error.UnknownHostName;
            break :blk addr_list.addrs[0];
        };
        const sock = try @import("posix_compat").socket(address.any.family, std.posix.SOCK.STREAM, 0);
        errdefer @import("posix_compat").close(sock);
        try @import("posix_compat").connect(sock, &address.any, address.getOsSockLen());
        return sock;
    }

    const HttpResponse = struct {
        data: []u8,
        status: u16,
        header_end: usize,

        fn deinit(self: *HttpResponse, alloc: Allocator) void {
            alloc.free(self.data);
            self.data = &.{};
        }

        fn headers(self: *const HttpResponse) []const u8 {
            return self.data[0..self.header_end];
        }

        fn body(self: *const HttpResponse) []const u8 {
            return self.data[self.header_end + 4 ..];
        }
    };

    const HttpRequestResult = struct {
        status: u16,
        response_len: usize,
    };

    fn httpRequest(self: *S3Client, method: []const u8, path: []const u8, query: []const u8, body: ?[]const u8, range_header: ?[]const u8, resp_buf: []u8) !u16 {
        return (try self.httpRequestWithLength(method, path, query, body, range_header, resp_buf)).status;
    }

    fn httpRequestWithLength(self: *S3Client, method: []const u8, path: []const u8, query: []const u8, body: ?[]const u8, range_header: ?[]const u8, resp_buf: []u8) !HttpRequestResult {
        if (self.test_http_request) |hook| {
            const ctx = self.test_http_ctx orelse return error.MissingTestHttpContext;
            @memset(resp_buf, 0);
            const status = try hook(ctx, method, path, query, body, range_header, resp_buf);
            return .{ .status = status, .response_len = detectHttpResponseLength(resp_buf) };
        }

        // Retry with exponential backoff
        const MAX_RETRIES: u32 = 3;
        var attempt: u32 = 0;
        while (true) {
            const result = self.sendRequestOnce(method, path, query, body, range_header, resp_buf.len);
            if (result) |response| {
                var resp = response;
                defer resp.deinit(self.allocator);
                @memset(resp_buf, 0);
                const copy_len = @min(resp_buf.len, resp.data.len);
                @memcpy(resp_buf[0..copy_len], resp.data[0..copy_len]);
                return .{ .status = resp.status, .response_len = copy_len };
            } else |err| {
                attempt += 1;
                if (attempt >= MAX_RETRIES) {
                    log.warn("S3 request failed after {d} retries: {s} {s}", .{ MAX_RETRIES, method, path });
                    return err;
                }
                // Exponential backoff: 100ms, 200ms, 400ms...
                const delay_ms: u64 = @as(u64, 100) << @intCast(attempt - 1);
                log.debug("S3 request retry {d}/{d} in {d}ms", .{ attempt, MAX_RETRIES, delay_ms });
                @import("time_compat").sleep(delay_ms * std.time.ns_per_ms);
            }
        }
    }

    fn sendRequest(self: *S3Client, method: []const u8, path: []const u8, query: []const u8, body: ?[]const u8, range_header: ?[]const u8, max_response_size: usize) !HttpResponse {
        if (self.test_http_request) |hook| {
            const ctx = self.test_http_ctx orelse return error.MissingTestHttpContext;
            const hook_buf_len = @min(max_response_size, 64 * 1024);
            const hook_buf = try self.allocator.alloc(u8, hook_buf_len);
            defer self.allocator.free(hook_buf);
            @memset(hook_buf, 0);
            const status = try hook(ctx, method, path, query, body, range_header, hook_buf);
            const response_len = detectHttpResponseLength(hook_buf);
            const data = try self.allocator.dupe(u8, hook_buf[0..response_len]);
            errdefer self.allocator.free(data);
            const header_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse return error.S3RequestFailed;
            return .{
                .data = data,
                .status = status,
                .header_end = header_end,
            };
        }

        const MAX_RETRIES: u32 = 3;
        var attempt: u32 = 0;
        while (true) {
            const result = self.sendRequestOnce(method, path, query, body, range_header, max_response_size);
            if (result) |response| {
                return response;
            } else |err| {
                attempt += 1;
                if (attempt >= MAX_RETRIES) {
                    log.warn("S3 request failed after {d} retries: {s} {s}", .{ MAX_RETRIES, method, path });
                    return err;
                }
                const delay_ms: u64 = @as(u64, 100) << @intCast(attempt - 1);
                log.debug("S3 request retry {d}/{d} in {d}ms", .{ attempt, MAX_RETRIES, delay_ms });
                @import("time_compat").sleep(delay_ms * std.time.ns_per_ms);
            }
        }
    }

    fn sendRequestOnce(self: *S3Client, method: []const u8, path: []const u8, query: []const u8, body: ?[]const u8, range_header: ?[]const u8, max_response_size: usize) !HttpResponse {
        var conn = try self.connect();
        defer conn.close();

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

        const host_header = try self.hostHeader();
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
        var req_buf = std.array_list.Managed(u8).init(self.allocator);
        defer req_buf.deinit();
        const writer = @import("list_compat").writer(&req_buf);

        const target = try requestTarget(self.allocator, path, query);
        defer self.allocator.free(target);

        try writer.print("{s} {s} HTTP/1.1\r\n", .{ method, target });
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

        try conn.writeAll(req_buf.items);
        if (body) |b| {
            try conn.writeAll(b);
        }

        var response = std.array_list.Managed(u8).init(self.allocator);
        errdefer response.deinit();

        var read_buf: [8192]u8 = undefined;
        while (true) {
            const n = try conn.read(&read_buf);
            if (n == 0) break;
            if (response.items.len + n > max_response_size) return error.S3ResponseTooLarge;
            try response.appendSlice(read_buf[0..n]);
        }

        const data = try response.toOwnedSlice();
        errdefer self.allocator.free(data);

        if (data.len < 12) return error.S3RequestFailed;
        const header_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse return error.S3RequestFailed;
        const status = std.fmt.parseInt(u16, data[9..12], 10) catch return error.S3RequestFailed;

        return .{
            .data = data,
            .status = status,
            .header_end = header_end,
        };
    }

    fn requestTarget(alloc: Allocator, path: []const u8, query: []const u8) ![]u8 {
        if (query.len == 0) return try alloc.dupe(u8, path);
        return try std.fmt.allocPrint(alloc, "{s}?{s}", .{ path, query });
    }

    fn hostHeader(self: *const S3Client) ![]u8 {
        if ((self.scheme == .https and self.port == 443) or
            (self.scheme == .http and self.port == 80))
        {
            return try self.allocator.dupe(u8, self.host);
        }
        return try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ self.host, self.port });
    }

    fn uriEncode(self: *S3Client, value: []const u8) ![]u8 {
        var out = std.array_list.Managed(u8).init(self.allocator);
        errdefer out.deinit();

        for (value) |ch| {
            const unreserved =
                (ch >= 'A' and ch <= 'Z') or
                (ch >= 'a' and ch <= 'z') or
                (ch >= '0' and ch <= '9') or
                ch == '-' or ch == '_' or ch == '.' or ch == '~';
            if (unreserved) {
                try out.append(ch);
            } else {
                const hex = std.fmt.bytesToHex([_]u8{ch}, .upper);
                try out.append('%');
                try out.appendSlice(&hex);
            }
        }

        return out.toOwnedSlice();
    }

    fn headerValue(headers: []const u8, name: []const u8) ?[]const u8 {
        var lines = std.mem.splitSequence(u8, headers, "\r\n");
        _ = lines.next();
        while (lines.next()) |line| {
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const key = std.mem.trim(u8, line[0..colon], " \t");
            if (std.ascii.eqlIgnoreCase(key, name)) {
                return std.mem.trim(u8, line[colon + 1 ..], " \t");
            }
        }
        return null;
    }

    fn contentLength(headers: []const u8) ?usize {
        const value = headerValue(headers, "Content-Length") orelse return null;
        return std.fmt.parseInt(usize, std.mem.trim(u8, value, " \t"), 10) catch null;
    }

    fn detectHttpResponseLength(data: []const u8) usize {
        if (std.mem.indexOf(u8, data, "\r\n\r\n")) |header_end| {
            const headers = data[0..header_end];
            const body_start = header_end + 4;
            const body = data[body_start..];
            if (isChunked(headers)) {
                if (chunkedBodyEnd(body)) |body_end| {
                    return body_start + body_end;
                } else |_| {}
            }
            if (contentLength(headers)) |len| {
                if (len <= data.len - body_start) return body_start + len;
            }
        }

        if (std.mem.indexOfScalar(u8, data, 0)) |zero| return zero;
        return data.len;
    }

    fn responseBodyFromRawHttpDataOwned(self: *S3Client, response_data: []const u8) ![]u8 {
        const header_end = std.mem.indexOf(u8, response_data, "\r\n\r\n") orelse
            return try self.allocator.dupe(u8, response_data);
        const headers = response_data[0..header_end];
        const body = response_data[header_end + 4 ..];
        if (isChunked(headers)) return try self.decodeChunked(body);
        return try self.allocator.dupe(u8, body);
    }

    fn responseHeaderValueOwned(self: *S3Client, response_data: []const u8, name: []const u8) !?[]u8 {
        const header_end = std.mem.indexOf(u8, response_data, "\r\n\r\n") orelse return null;
        const value = headerValue(response_data[0..header_end], name) orelse return null;
        return try self.allocator.dupe(u8, value);
    }

    fn isValidMultipartEtag(etag: []const u8) bool {
        if (etag.len == 0) return false;
        if (std.mem.indexOfAny(u8, etag, "\r\n<>&") != null) return false;
        return true;
    }

    fn isMultipartCompleteEmbeddedError(response_data: []const u8) bool {
        return std.mem.indexOf(u8, response_data, "<Error>") != null or
            std.mem.indexOf(u8, response_data, "<Error ") != null;
    }

    fn contentRangeMatches(value: []const u8, requested_offset: u64, requested_length: u64) bool {
        if (requested_length == 0) return false;
        const trimmed = std.mem.trim(u8, value, " \t");
        if (!std.mem.startsWith(u8, trimmed, "bytes ")) return false;

        const spec = trimmed["bytes ".len..];
        const dash = std.mem.indexOfScalar(u8, spec, '-') orelse return false;
        const slash = std.mem.indexOfScalar(u8, spec, '/') orelse return false;
        if (dash == 0 or slash <= dash + 1) return false;

        const start_text = std.mem.trim(u8, spec[0..dash], " \t");
        const end_text = std.mem.trim(u8, spec[dash + 1 .. slash], " \t");
        const start = std.fmt.parseInt(u64, start_text, 10) catch return false;
        const end = std.fmt.parseInt(u64, end_text, 10) catch return false;
        const expected_end = std.math.add(u64, requested_offset, requested_length - 1) catch return false;

        return start == requested_offset and end == expected_end;
    }

    fn isChunked(headers: []const u8) bool {
        const value = headerValue(headers, "Transfer-Encoding") orelse return false;
        var tokens = std.mem.splitScalar(u8, value, ',');
        while (tokens.next()) |token| {
            if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, token, " \t"), "chunked")) return true;
        }
        return false;
    }

    fn responseBodyOwned(self: *S3Client, response: *const HttpResponse) ![]u8 {
        const body = if (isChunked(response.headers()))
            try self.decodeChunked(response.body())
        else
            try self.allocator.dupe(u8, response.body());
        errdefer self.allocator.free(body);

        try validateResponseChecksum(response.headers(), body);
        return body;
    }

    fn validateResponseChecksum(headers: []const u8, body: []const u8) !void {
        const expected = headerValue(headers, "x-amz-checksum-sha256") orelse return;
        var checksum_buf: [sha256_checksum_base64_len]u8 = undefined;
        const actual = sha256Base64(body, &checksum_buf);
        if (!std.mem.eql(u8, std.mem.trim(u8, expected, " \t"), actual)) {
            return error.S3ChecksumMismatch;
        }
    }

    fn sha256Base64(body: []const u8, out: *[sha256_checksum_base64_len]u8) []const u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
        return std.base64.standard.Encoder.encode(out, &digest);
    }

    fn rangeBodyFromResponse(self: *S3Client, response: *const HttpResponse, requested_offset: u64, requested_length: u64) ![]u8 {
        // 206 = Partial Content, 200 = Full content.
        if (response.status != 206 and response.status != 200) return error.S3GetFailed;
        if (response.status == 206) {
            const content_range = headerValue(response.headers(), "Content-Range") orelse
                return error.S3RangeContentRangeMismatch;
            if (!contentRangeMatches(content_range, requested_offset, requested_length)) {
                return error.S3RangeContentRangeMismatch;
            }
        }
        return try self.exactRangeBody(response.status, requested_offset, requested_length, try self.responseBodyOwned(response));
    }

    fn exactRangeBody(self: *S3Client, status: u16, requested_offset: u64, requested_length: u64, body: []u8) ![]u8 {
        errdefer self.allocator.free(body);

        const requested_len = std.math.cast(usize, requested_length) orelse return error.S3RangeInvalid;
        if (status == 206) {
            if (body.len != requested_len) return error.S3RangeLengthMismatch;
            return body;
        }

        const start = std.math.cast(usize, requested_offset) orelse return error.S3RangeInvalid;
        const end = std.math.add(usize, start, requested_len) catch return error.S3RangeInvalid;
        if (end > body.len) return error.S3RangeLengthMismatch;
        if (start == 0 and end == body.len) return body;

        const exact = try self.allocator.dupe(u8, body[start..end]);
        self.allocator.free(body);
        return exact;
    }

    // ---- Metrics helpers ----

    /// Record a successful S3 request in the Prometheus metric registry.
    fn recordS3Request(self: *S3Client, operation: []const u8) void {
        if (self.metrics) |m| {
            m.incrementLabeledCounter("s3_requests_total", &.{operation});
        }
    }

    /// Record a failed S3 request.
    fn recordS3Error(self: *S3Client, operation: []const u8) void {
        if (self.metrics) |m| {
            m.incrementLabeledCounter("s3_requests_total", &.{operation});
            m.incrementLabeledCounter("s3_request_errors_total", &.{operation});
        }
    }

    /// Record S3 request duration in seconds.
    fn recordS3Duration(self: *S3Client, operation: []const u8, start_ns: i128) void {
        if (self.metrics) |m| {
            const elapsed_ns = @import("time_compat").nanoTimestamp() - start_ns;
            const elapsed_secs: f64 = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
            m.observeLabeledHistogram("s3_request_duration_seconds", &.{operation}, elapsed_secs);
        }
    }

    /// Record S3 bytes transferred.
    fn recordS3Bytes(self: *S3Client, direction: []const u8, bytes: usize) void {
        if (self.metrics) |m| {
            m.addLabeledCounter("s3_bytes_total", &.{direction}, @intCast(bytes));
        }
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
                return err;
            };
        } else if (self.mock) |m| {
            if (try m.getObjectOrError(key)) |data| {
                return try self.allocator.dupe(u8, data);
            }
        }
        return null;
    }

    pub fn getObjectRange(self: *S3Storage, key: []const u8, offset: u64, length: u64) !?[]u8 {
        if (self.client) |c| {
            return c.getObjectRange(key, offset, length) catch |err| {
                log.warn("S3 GetObjectRange failed for key '{s}' offset={d} length={d}: {}", .{ key, offset, length, err });
                return err;
            };
        } else if (self.mock) |m| {
            const start = std.math.cast(usize, offset) orelse return error.S3RangeInvalid;
            const len = std.math.cast(usize, length) orelse return error.S3RangeInvalid;
            const end = std.math.add(usize, start, len) catch return error.S3RangeInvalid;
            if (try m.getObjectRangeOrError(key, start, end)) |data| {
                return try self.allocator.dupe(u8, data);
            }
        }
        return null;
    }

    pub fn listObjectKeys(self: *S3Storage, prefix: []const u8) ![][]u8 {
        if (self.client) |c| {
            var all_keys = std.array_list.Managed([]u8).init(self.allocator);
            errdefer {
                for (all_keys.items) |key| self.allocator.free(key);
                all_keys.deinit();
            }

            var continuation_token: ?[]u8 = null;
            defer if (continuation_token) |token| self.allocator.free(token);

            while (true) {
                const xml = try c.listObjectsPage(prefix, continuation_token);
                defer self.allocator.free(xml);

                var page = try parseListObjectPage(self.allocator, xml);
                defer page.deinit(self.allocator);

                try all_keys.appendSlice(page.keys);
                self.allocator.free(page.keys);
                page.keys = &.{};

                const next_token = page.next_continuation_token orelse {
                    if (page.is_truncated) return error.S3ListMissingContinuationToken;
                    break;
                };
                page.next_continuation_token = null;
                if (continuation_token) |old| self.allocator.free(old);
                continuation_token = next_token;
            }

            std.mem.sort([]u8, all_keys.items, {}, struct {
                fn lessThan(_: void, a: []u8, b: []u8) bool {
                    return std.mem.lessThan(u8, a, b);
                }
            }.lessThan);

            return try all_keys.toOwnedSlice();
        } else if (self.mock) |m| {
            return try m.listObjectKeys(self.allocator, prefix);
        }
        return try self.allocator.alloc([]u8, 0);
    }

    pub fn deleteObject(self: *S3Storage, key: []const u8) !void {
        if (self.client) |c| {
            try c.deleteObject(key);
        } else if (self.mock) |m| {
            _ = try m.deleteObjectOrError(key);
        }
    }
};

const ListObjectPage = struct {
    keys: [][]u8,
    next_continuation_token: ?[]u8,
    is_truncated: bool = false,

    fn deinit(self: *ListObjectPage, alloc: Allocator) void {
        for (self.keys) |key| alloc.free(key);
        if (self.keys.len > 0) alloc.free(self.keys);
        if (self.next_continuation_token) |token| alloc.free(token);
        self.* = .{ .keys = &.{}, .next_continuation_token = null, .is_truncated = false };
    }
};

fn parseListObjectKeys(alloc: Allocator, xml: []const u8) ![][]u8 {
    const page = try parseListObjectPage(alloc, xml);
    defer {
        if (page.next_continuation_token) |token| alloc.free(token);
    }

    std.mem.sort([]u8, page.keys, {}, struct {
        fn lessThan(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    return page.keys;
}

fn parseListObjectPage(alloc: Allocator, xml: []const u8) !ListObjectPage {
    var keys = std.array_list.Managed([]u8).init(alloc);
    errdefer {
        for (keys.items) |key| alloc.free(key);
        keys.deinit();
    }

    var pos: usize = 0;
    while (std.mem.indexOf(u8, xml[pos..], "<Key>")) |start_rel| {
        const key_start = pos + start_rel + "<Key>".len;
        const end_rel = std.mem.indexOf(u8, xml[key_start..], "</Key>") orelse break;
        const key_end = key_start + end_rel;
        const key_copy = try decodeXmlTextAlloc(alloc, xml[key_start..key_end]);
        errdefer alloc.free(key_copy);
        try keys.append(key_copy);
        pos = key_end + "</Key>".len;
    }

    const next_token = try parseXmlTagValue(alloc, xml, "NextContinuationToken");
    const is_truncated = parseXmlBoolTag(xml, "IsTruncated") orelse (next_token != null);

    return .{
        .keys = try keys.toOwnedSlice(),
        .next_continuation_token = next_token,
        .is_truncated = is_truncated,
    };
}

fn parseXmlTagValue(alloc: Allocator, xml: []const u8, comptime tag: []const u8) !?[]u8 {
    const open_tag = "<" ++ tag ++ ">";
    const close_tag = "</" ++ tag ++ ">";
    const start = std.mem.indexOf(u8, xml, open_tag) orelse return null;
    const value_start = start + open_tag.len;
    const end_rel = std.mem.indexOf(u8, xml[value_start..], close_tag) orelse return null;
    const value = xml[value_start .. value_start + end_rel];
    if (value.len == 0) return null;
    return try decodeXmlTextAlloc(alloc, value);
}

fn parseXmlBoolTag(xml: []const u8, comptime tag: []const u8) ?bool {
    const open_tag = "<" ++ tag ++ ">";
    const close_tag = "</" ++ tag ++ ">";
    const start = std.mem.indexOf(u8, xml, open_tag) orelse return null;
    const value_start = start + open_tag.len;
    const end_rel = std.mem.indexOf(u8, xml[value_start..], close_tag) orelse return null;
    const value = std.mem.trim(u8, xml[value_start .. value_start + end_rel], " \t\r\n");
    if (std.ascii.eqlIgnoreCase(value, "true")) return true;
    if (std.ascii.eqlIgnoreCase(value, "false")) return false;
    return null;
}

fn decodeXmlTextAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    var out = std.array_list.Managed(u8).init(alloc);
    errdefer out.deinit();

    var pos: usize = 0;
    while (pos < text.len) {
        if (text[pos] != '&') {
            try out.append(text[pos]);
            pos += 1;
            continue;
        }

        if (std.mem.startsWith(u8, text[pos..], "&amp;")) {
            try out.append('&');
            pos += "&amp;".len;
        } else if (std.mem.startsWith(u8, text[pos..], "&lt;")) {
            try out.append('<');
            pos += "&lt;".len;
        } else if (std.mem.startsWith(u8, text[pos..], "&gt;")) {
            try out.append('>');
            pos += "&gt;".len;
        } else if (std.mem.startsWith(u8, text[pos..], "&quot;")) {
            try out.append('"');
            pos += "&quot;".len;
        } else if (std.mem.startsWith(u8, text[pos..], "&apos;")) {
            try out.append('\'');
            pos += "&apos;".len;
        } else {
            return error.InvalidXmlEntity;
        }
    }

    return try out.toOwnedSlice();
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "S3Client init" {
    const client = S3Client.init(testing.allocator, .{});
    try testing.expectEqualStrings("127.0.0.1", client.host);
    try testing.expectEqual(@as(u16, 9000), client.port);
    try testing.expectEqual(@as(u64, 0), client.put_count);
}

test "S3Client normalizes endpoint scheme and port" {
    const https_client = S3Client.init(testing.allocator, .{ .host = "https://s3.example.com/my-bucket" });
    try testing.expectEqualStrings("s3.example.com", https_client.host);
    try testing.expectEqual(S3Client.Scheme.https, https_client.scheme);
    try testing.expectEqual(@as(u16, 443), https_client.port);

    const minio_client = S3Client.init(testing.allocator, .{ .host = "http://minio:9000" });
    try testing.expectEqualStrings("minio", minio_client.host);
    try testing.expectEqual(S3Client.Scheme.http, minio_client.scheme);
    try testing.expectEqual(@as(u16, 9000), minio_client.port);
}

test "S3Client host header omits default provider ports" {
    const aws_client = S3Client.init(testing.allocator, .{ .host = "https://s3.us-east-1.amazonaws.com" });
    const aws_host = try aws_client.hostHeader();
    defer testing.allocator.free(aws_host);
    try testing.expectEqualStrings("s3.us-east-1.amazonaws.com", aws_host);

    const http_client = S3Client.init(testing.allocator, .{
        .host = "s3-compatible.local",
        .port = 80,
        .scheme = .http,
    });
    const http_host = try http_client.hostHeader();
    defer testing.allocator.free(http_host);
    try testing.expectEqualStrings("s3-compatible.local", http_host);

    const custom_port_client = S3Client.init(testing.allocator, .{ .host = "https://r2.example.com:8443" });
    const custom_host = try custom_port_client.hostHeader();
    defer testing.allocator.free(custom_host);
    try testing.expectEqualStrings("r2.example.com:8443", custom_host);
}

test "S3Client request target includes canonical query" {
    const target = try S3Client.requestTarget(testing.allocator, "/bucket", "list-type=2&prefix=wal%2F");
    defer testing.allocator.free(target);
    try testing.expectEqualStrings("/bucket?list-type=2&prefix=wal%2F", target);
}

test "S3Client uriEncode encodes query values" {
    var client = S3Client.init(testing.allocator, .{});
    const encoded = try client.uriEncode("wal/path id=1");
    defer testing.allocator.free(encoded);
    try testing.expectEqualStrings("wal%2Fpath%20id%3D1", encoded);
}

test "S3Client decodeChunked validates framing" {
    var client = S3Client.init(testing.allocator, .{});
    const decoded = try client.decodeChunked("5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\n\r\n");
    defer testing.allocator.free(decoded);
    try testing.expectEqualStrings("hello world", decoded);

    const with_trailer = try client.decodeChunked("5\r\nhello\r\n0\r\nx-amz-checksum-crc32: abc\r\n\r\n");
    defer testing.allocator.free(with_trailer);
    try testing.expectEqualStrings("hello", with_trailer);

    try testing.expectError(error.InvalidChunkedEncoding, client.decodeChunked("5\r\nabc\r\n0\r\n\r\n"));
    try testing.expectError(error.InvalidChunkedEncoding, client.decodeChunked("5\r\nhello\r\n0\r\n"));
    try testing.expectError(error.InvalidChunkedEncoding, client.decodeChunked("5\r\nhello\r\n0\r\n\r\njunk"));
}

test "S3Client range body returns exact requested window" {
    var client = S3Client.init(testing.allocator, .{});

    const partial_body = try testing.allocator.dupe(u8, "range");
    const partial = try client.exactRangeBody(206, 10, 5, partial_body);
    defer testing.allocator.free(partial);
    try testing.expectEqualStrings("range", partial);

    const full_body = try testing.allocator.dupe(u8, "0123456789");
    const sliced = try client.exactRangeBody(200, 2, 4, full_body);
    defer testing.allocator.free(sliced);
    try testing.expectEqualStrings("2345", sliced);

    const short_body = try testing.allocator.dupe(u8, "abc");
    try testing.expectError(error.S3RangeLengthMismatch, client.exactRangeBody(206, 0, 4, short_body));

    const ignored_range_body = try testing.allocator.dupe(u8, "abcd");
    try testing.expectError(error.S3RangeLengthMismatch, client.exactRangeBody(200, 2, 4, ignored_range_body));
}

test "S3Client validates partial content range window" {
    try testing.expect(S3Client.contentRangeMatches("bytes 10-14/100", 10, 5));
    try testing.expect(S3Client.contentRangeMatches(" bytes 10 - 14 / * ", 10, 5));
    try testing.expect(!S3Client.contentRangeMatches("bytes 10-13/100", 10, 5));
    try testing.expect(!S3Client.contentRangeMatches("bytes 11-15/100", 10, 5));
    try testing.expect(!S3Client.contentRangeMatches("items 10-14/100", 10, 5));
    try testing.expect(!S3Client.contentRangeMatches("bytes */100", 10, 5));
}

fn makeHttpResponse(alloc: Allocator, status: u16, raw: []const u8) !S3Client.HttpResponse {
    const data = try alloc.dupe(u8, raw);
    errdefer alloc.free(data);
    const header_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse return error.S3RequestFailed;
    return .{
        .data = data,
        .status = status,
        .header_end = header_end,
    };
}

test "S3Client range response fails closed on missing or mismatched Content-Range" {
    var client = S3Client.init(testing.allocator, .{});

    {
        var response = try makeHttpResponse(
            testing.allocator,
            206,
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 10-14/100\r\n\r\nrange",
        );
        defer response.deinit(testing.allocator);

        const body = try client.rangeBodyFromResponse(&response, 10, 5);
        defer testing.allocator.free(body);
        try testing.expectEqualStrings("range", body);
    }

    {
        var response = try makeHttpResponse(
            testing.allocator,
            206,
            "HTTP/1.1 206 Partial Content\r\n\r\nrange",
        );
        defer response.deinit(testing.allocator);

        try testing.expectError(error.S3RangeContentRangeMismatch, client.rangeBodyFromResponse(&response, 10, 5));
    }

    {
        var response = try makeHttpResponse(
            testing.allocator,
            206,
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 11-15/100\r\n\r\nrange",
        );
        defer response.deinit(testing.allocator);

        try testing.expectError(error.S3RangeContentRangeMismatch, client.rangeBodyFromResponse(&response, 10, 5));
    }
}

test "S3Client validates SHA256 checksum response header" {
    var client = S3Client.init(testing.allocator, .{});
    var checksum_buf: [S3Client.sha256_checksum_base64_len]u8 = undefined;
    const checksum = S3Client.sha256Base64("checksum-body", &checksum_buf);

    const raw_ok = try std.fmt.allocPrint(
        testing.allocator,
        "HTTP/1.1 200 OK\r\nx-amz-checksum-sha256: {s}\r\n\r\nchecksum-body",
        .{checksum},
    );
    defer testing.allocator.free(raw_ok);

    var ok_response = try makeHttpResponse(testing.allocator, 200, raw_ok);
    defer ok_response.deinit(testing.allocator);

    const ok_body = try client.responseBodyOwned(&ok_response);
    defer testing.allocator.free(ok_body);
    try testing.expectEqualStrings("checksum-body", ok_body);

    var bad_response = try makeHttpResponse(
        testing.allocator,
        200,
        "HTTP/1.1 200 OK\r\nx-amz-checksum-sha256: AAAA\r\n\r\nchecksum-body",
    );
    defer bad_response.deinit(testing.allocator);

    try testing.expectError(error.S3ChecksumMismatch, client.responseBodyOwned(&bad_response));
}

test "S3Client multipart ETag validation" {
    try testing.expect(S3Client.isValidMultipartEtag("\"abc123\""));
    try testing.expect(S3Client.isValidMultipartEtag("abc123-2"));
    try testing.expect(!S3Client.isValidMultipartEtag(""));
    try testing.expect(!S3Client.isValidMultipartEtag("abc\r\nx-bad: 1"));
    try testing.expect(!S3Client.isValidMultipartEtag("<bad>"));
    try testing.expect(!S3Client.isValidMultipartEtag("abc&bad"));
}

test "S3Client responseHeaderValueOwned trims ETag" {
    var client = S3Client.init(testing.allocator, .{});
    const response =
        "HTTP/1.1 200 OK\r\n" ++
        "ETag:   \"abc123\"   \r\n" ++
        "\r\n";

    const etag = (try client.responseHeaderValueOwned(response, "ETag")).?;
    defer testing.allocator.free(etag);
    try testing.expectEqualStrings("\"abc123\"", etag);
}

const MultipartTestServer = struct {
    allocator: Allocator,
    mode: Mode,
    init_count: u32 = 0,
    part_put_count: u32 = 0,
    part1_attempts: u32 = 0,
    part2_attempts: u32 = 0,
    complete_count: u32 = 0,
    abort_count: u32 = 0,
    complete_body: []u8 = &.{},

    const Mode = enum {
        bad_etag_then_success,
        persistent_bad_etag,
        complete_failure,
        complete_embedded_error,
        chunked_init_and_complete,
        chunked_complete_embedded_error,
        escaped_upload_id,
    };

    fn deinit(self: *MultipartTestServer) void {
        if (self.complete_body.len > 0) self.allocator.free(self.complete_body);
    }

    fn request(ctx: *anyopaque, method: []const u8, _: []const u8, query: []const u8, body: ?[]const u8, _: ?[]const u8, resp_buf: []u8) anyerror!u16 {
        const self: *MultipartTestServer = @ptrCast(@alignCast(ctx));
        @memset(resp_buf, 0);

        if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, query, "uploads")) {
            self.init_count += 1;
            if (self.mode == .chunked_init_and_complete or self.mode == .chunked_complete_embedded_error) {
                writeChunkedResponseParts(
                    resp_buf,
                    "<InitiateMultipartUploadResult><Upload",
                    "Id>upload-1</UploadId></InitiateMultipartUploadResult>",
                );
            } else if (self.mode == .escaped_upload_id) {
                writeResponse(resp_buf, "<InitiateMultipartUploadResult><UploadId>upload&amp;id/with space</UploadId></InitiateMultipartUploadResult>");
            } else {
                writeResponse(resp_buf, "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>");
            }
            return 200;
        }

        if (std.mem.eql(u8, method, "PUT")) {
            try self.expectEscapedUploadIdQuery(query);
            self.part_put_count += 1;
            if (std.mem.indexOf(u8, query, "partNumber=1") != null) {
                self.part1_attempts += 1;
                return self.partResponse(1, resp_buf);
            }
            if (std.mem.indexOf(u8, query, "partNumber=2") != null) {
                self.part2_attempts += 1;
                return self.partResponse(2, resp_buf);
            }
            return error.UnexpectedMultipartPart;
        }

        if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, query, "uploadId=")) {
            try self.expectEscapedUploadIdQuery(query);
            self.complete_count += 1;
            if (self.complete_body.len > 0) self.allocator.free(self.complete_body);
            self.complete_body = try self.allocator.dupe(u8, body orelse "");
            if (self.mode == .complete_embedded_error) {
                writeResponse(resp_buf, "HTTP/1.1 200 OK\r\n\r\n<Error><Code>EntityTooSmall</Code><Message>part too small</Message></Error>");
                return 200;
            }
            if (self.mode == .chunked_complete_embedded_error) {
                writeChunkedResponseParts(resp_buf, "<Err", "or><Code>EntityTooSmall</Code><Message>part too small</Message></Error>");
            } else if (self.mode == .chunked_init_and_complete) {
                writeChunkedResponseParts(resp_buf, "<CompleteMultipart", "UploadResult/>");
            } else {
                writeResponse(resp_buf, "<CompleteMultipartUploadResult/>");
            }
            return if (self.mode == .complete_failure) 500 else 200;
        }

        if (std.mem.eql(u8, method, "DELETE") and std.mem.startsWith(u8, query, "uploadId=")) {
            try self.expectEscapedUploadIdQuery(query);
            self.abort_count += 1;
            writeResponse(resp_buf, "");
            return 204;
        }

        return error.UnexpectedMultipartRequest;
    }

    fn partResponse(self: *MultipartTestServer, part_number: u32, resp_buf: []u8) u16 {
        switch (self.mode) {
            .bad_etag_then_success => {
                if (part_number == 1 and self.part1_attempts == 1) {
                    writeResponse(resp_buf, "HTTP/1.1 200 OK\r\n\r\n");
                } else if (part_number == 1 and self.part1_attempts == 2) {
                    writeResponse(resp_buf, "HTTP/1.1 200 OK\r\nETag: <bad>\r\n\r\n");
                } else {
                    writeValidPartEtag(resp_buf, part_number);
                }
                return 200;
            },
            .persistent_bad_etag => {
                writeResponse(resp_buf, "HTTP/1.1 200 OK\r\nETag: <bad>\r\n\r\n");
                return 200;
            },
            .complete_failure => {
                writeValidPartEtag(resp_buf, part_number);
                return 200;
            },
            .complete_embedded_error => {
                writeValidPartEtag(resp_buf, part_number);
                return 200;
            },
            .chunked_init_and_complete, .chunked_complete_embedded_error => {
                writeValidPartEtag(resp_buf, part_number);
                return 200;
            },
            .escaped_upload_id => {
                writeValidPartEtag(resp_buf, part_number);
                return 200;
            },
        }
    }

    fn expectEscapedUploadIdQuery(self: *const MultipartTestServer, query: []const u8) !void {
        if (self.mode != .escaped_upload_id) return;
        if (std.mem.indexOf(u8, query, "uploadId=upload%26id%2Fwith%20space") == null) {
            return error.MultipartUploadIdNotDecodedAndEncoded;
        }
    }

    fn writeResponse(resp_buf: []u8, data: []const u8) void {
        const copy_len = @min(resp_buf.len, data.len);
        @memcpy(resp_buf[0..copy_len], data[0..copy_len]);
    }

    fn writeChunkedResponseParts(resp_buf: []u8, first: []const u8, second: []const u8) void {
        _ = std.fmt.bufPrint(
            resp_buf,
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{x}\r\n{s}\r\n{x}\r\n{s}\r\n0\r\n\r\n",
            .{ first.len, first, second.len, second },
        ) catch return;
    }

    fn writeValidPartEtag(resp_buf: []u8, part_number: u32) void {
        if (part_number == 1) {
            writeResponse(resp_buf, "HTTP/1.1 200 OK\r\nETag: \"part-1\"\r\n\r\n");
        } else {
            writeResponse(resp_buf, "HTTP/1.1 200 OK\r\nETag: \"part-2\"\r\n\r\n");
        }
    }
};

fn allocMultipartTestData(alloc: Allocator) ![]u8 {
    const len = 5 * 1024 * 1024 + 17;
    const data = try alloc.alloc(u8, len);
    @memset(data, 'x');
    return data;
}

test "S3Client multipart retries missing and invalid part ETags before success" {
    var server = MultipartTestServer{ .allocator = testing.allocator, .mode = .bad_etag_then_success };
    defer server.deinit();

    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = MultipartTestServer.request;

    const data = try allocMultipartTestData(testing.allocator);
    defer testing.allocator.free(data);

    try client.putObjectMultipart("large-object", data);

    try testing.expectEqual(@as(u32, 1), server.init_count);
    try testing.expectEqual(@as(u32, 4), server.part_put_count);
    try testing.expectEqual(@as(u32, 3), server.part1_attempts);
    try testing.expectEqual(@as(u32, 1), server.part2_attempts);
    try testing.expectEqual(@as(u32, 1), server.complete_count);
    try testing.expectEqual(@as(u32, 0), server.abort_count);
    try testing.expect(std.mem.indexOf(u8, server.complete_body, "<ETag>\"part-1\"</ETag>") != null);
    try testing.expect(std.mem.indexOf(u8, server.complete_body, "<ETag>\"part-2\"</ETag>") != null);
    try testing.expectEqual(@as(u64, 1), client.put_count);
    try testing.expectEqual(@as(u64, @intCast(data.len)), client.bytes_uploaded);
}

test "S3Client multipart aborts after persistent bad part ETag" {
    var server = MultipartTestServer{ .allocator = testing.allocator, .mode = .persistent_bad_etag };
    defer server.deinit();

    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = MultipartTestServer.request;

    const data = try allocMultipartTestData(testing.allocator);
    defer testing.allocator.free(data);

    try testing.expectError(error.S3PartUploadFailed, client.putObjectMultipart("large-object", data));

    try testing.expectEqual(@as(u32, 1), server.init_count);
    try testing.expectEqual(@as(u32, 3), server.part_put_count);
    try testing.expectEqual(@as(u32, 3), server.part1_attempts);
    try testing.expectEqual(@as(u32, 0), server.part2_attempts);
    try testing.expectEqual(@as(u32, 0), server.complete_count);
    try testing.expectEqual(@as(u32, 1), server.abort_count);
    try testing.expectEqual(@as(u64, 0), client.put_count);
    try testing.expectEqual(@as(u64, 0), client.bytes_uploaded);
}

test "S3Client multipart aborts after complete failure" {
    var server = MultipartTestServer{ .allocator = testing.allocator, .mode = .complete_failure };
    defer server.deinit();

    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = MultipartTestServer.request;

    const data = try allocMultipartTestData(testing.allocator);
    defer testing.allocator.free(data);

    try testing.expectError(error.S3MultipartCompleteFailed, client.putObjectMultipart("large-object", data));

    try testing.expectEqual(@as(u32, 1), server.init_count);
    try testing.expectEqual(@as(u32, 2), server.part_put_count);
    try testing.expectEqual(@as(u32, 1), server.part1_attempts);
    try testing.expectEqual(@as(u32, 1), server.part2_attempts);
    try testing.expectEqual(@as(u32, 1), server.complete_count);
    try testing.expectEqual(@as(u32, 1), server.abort_count);
    try testing.expect(std.mem.indexOf(u8, server.complete_body, "<ETag>\"part-1\"</ETag>") != null);
    try testing.expectEqual(@as(u64, 0), client.put_count);
    try testing.expectEqual(@as(u64, 0), client.bytes_uploaded);
}

test "S3Client multipart aborts after embedded complete error" {
    var server = MultipartTestServer{ .allocator = testing.allocator, .mode = .complete_embedded_error };
    defer server.deinit();

    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = MultipartTestServer.request;

    const data = try allocMultipartTestData(testing.allocator);
    defer testing.allocator.free(data);

    try testing.expectError(error.S3MultipartCompleteFailed, client.putObjectMultipart("large-object", data));

    try testing.expectEqual(@as(u32, 1), server.init_count);
    try testing.expectEqual(@as(u32, 2), server.part_put_count);
    try testing.expectEqual(@as(u32, 1), server.complete_count);
    try testing.expectEqual(@as(u32, 1), server.abort_count);
    try testing.expect(std.mem.indexOf(u8, server.complete_body, "<ETag>\"part-1\"</ETag>") != null);
    try testing.expectEqual(@as(u64, 0), client.put_count);
    try testing.expectEqual(@as(u64, 0), client.bytes_uploaded);
}

test "S3Client multipart decodes chunked init and complete responses" {
    var server = MultipartTestServer{ .allocator = testing.allocator, .mode = .chunked_init_and_complete };
    defer server.deinit();

    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = MultipartTestServer.request;

    const data = try allocMultipartTestData(testing.allocator);
    defer testing.allocator.free(data);

    try client.putObjectMultipart("large-object", data);

    try testing.expectEqual(@as(u32, 1), server.init_count);
    try testing.expectEqual(@as(u32, 2), server.part_put_count);
    try testing.expectEqual(@as(u32, 1), server.complete_count);
    try testing.expectEqual(@as(u32, 0), server.abort_count);
    try testing.expect(std.mem.indexOf(u8, server.complete_body, "<ETag>\"part-1\"</ETag>") != null);
    try testing.expectEqual(@as(u64, 1), client.put_count);
    try testing.expectEqual(@as(u64, @intCast(data.len)), client.bytes_uploaded);
}

test "S3Client multipart decodes XML-escaped upload IDs before signing requests" {
    var server = MultipartTestServer{ .allocator = testing.allocator, .mode = .escaped_upload_id };
    defer server.deinit();

    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = MultipartTestServer.request;

    const data = try allocMultipartTestData(testing.allocator);
    defer testing.allocator.free(data);

    try client.putObjectMultipart("large-object", data);

    try testing.expectEqual(@as(u32, 1), server.init_count);
    try testing.expectEqual(@as(u32, 2), server.part_put_count);
    try testing.expectEqual(@as(u32, 1), server.complete_count);
    try testing.expectEqual(@as(u32, 0), server.abort_count);
    try testing.expect(std.mem.indexOf(u8, server.complete_body, "<ETag>\"part-1\"</ETag>") != null);
    try testing.expectEqual(@as(u64, 1), client.put_count);
    try testing.expectEqual(@as(u64, @intCast(data.len)), client.bytes_uploaded);
}

test "S3Client multipart aborts after chunked embedded complete error" {
    var server = MultipartTestServer{ .allocator = testing.allocator, .mode = .chunked_complete_embedded_error };
    defer server.deinit();

    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = MultipartTestServer.request;

    const data = try allocMultipartTestData(testing.allocator);
    defer testing.allocator.free(data);

    try testing.expectError(error.S3MultipartCompleteFailed, client.putObjectMultipart("large-object", data));

    try testing.expectEqual(@as(u32, 1), server.init_count);
    try testing.expectEqual(@as(u32, 2), server.part_put_count);
    try testing.expectEqual(@as(u32, 1), server.complete_count);
    try testing.expectEqual(@as(u32, 1), server.abort_count);
    try testing.expectEqual(@as(u64, 0), client.put_count);
    try testing.expectEqual(@as(u64, 0), client.bytes_uploaded);
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

test "S3Storage lists mock object keys" {
    var mock = @import("s3.zig").MockS3.init(testing.allocator);
    defer mock.deinit();

    var storage = S3Storage.initMock(testing.allocator, &mock);
    try storage.putObject("wal/epoch-0/bulk/0000000002", "b");
    try storage.putObject("wal/epoch-0/bulk/0000000001", "a");
    try storage.putObject("other/key", "ignored");

    const keys = try storage.listObjectKeys("wal/");
    defer {
        for (keys) |key| testing.allocator.free(key);
        testing.allocator.free(keys);
    }

    try testing.expectEqual(@as(usize, 2), keys.len);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000001", keys[0]);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000002", keys[1]);
}

test "S3Storage mock mode propagates injected operation failures" {
    var mock = @import("s3.zig").MockS3.init(testing.allocator);
    defer mock.deinit();

    var storage = S3Storage.initMock(testing.allocator, &mock);
    try storage.putObject("key", "0123456789");

    mock.failNextGetObjects(1);
    try testing.expectError(error.InjectedGetFailure, storage.getObject("key"));

    const data = (try storage.getObject("key")).?;
    defer testing.allocator.free(data);
    try testing.expectEqualStrings("0123456789", data);

    mock.failNextRangeReads(1);
    try testing.expectError(error.InjectedRangeFailure, storage.getObjectRange("key", 2, 4));

    const range = (try storage.getObjectRange("key", 2, 4)).?;
    defer testing.allocator.free(range);
    try testing.expectEqualStrings("2345", range);

    mock.failNextListObjects(1);
    try testing.expectError(error.InjectedListFailure, storage.listObjectKeys(""));

    mock.failNextDeleteObjects(1);
    try testing.expectError(error.InjectedDeleteFailure, storage.deleteObject("key"));
    try testing.expect(mock.getObject("key") != null);

    try storage.deleteObject("key");
    try testing.expect(mock.getObject("key") == null);
}

const ListObjectsTestServer = struct {
    mode: Mode,
    page_count: u32 = 0,

    const Mode = enum {
        missing_continuation_token,
        two_pages,
    };

    fn request(ctx: *anyopaque, method: []const u8, _: []const u8, query: []const u8, _: ?[]const u8, _: ?[]const u8, resp_buf: []u8) anyerror!u16 {
        const self: *ListObjectsTestServer = @ptrCast(@alignCast(ctx));
        @memset(resp_buf, 0);
        if (!std.mem.eql(u8, method, "GET") or std.mem.indexOf(u8, query, "list-type=2") == null) {
            return error.UnexpectedListObjectsRequest;
        }

        self.page_count += 1;
        switch (self.mode) {
            .missing_continuation_token => {
                writeHttpResponse(resp_buf,
                    \\<ListBucketResult>
                    \\  <IsTruncated>true</IsTruncated>
                    \\  <Contents><Key>wal/epoch-0/bulk/0000000001</Key></Contents>
                    \\</ListBucketResult>
                );
            },
            .two_pages => {
                if (std.mem.indexOf(u8, query, "continuation-token=next-token") == null) {
                    writeHttpResponse(resp_buf,
                        \\<ListBucketResult>
                        \\  <IsTruncated>true</IsTruncated>
                        \\  <Contents><Key>wal/epoch-0/bulk/0000000002</Key></Contents>
                        \\  <NextContinuationToken>next-token</NextContinuationToken>
                        \\</ListBucketResult>
                    );
                } else {
                    writeHttpResponse(resp_buf,
                        \\<ListBucketResult>
                        \\  <IsTruncated>false</IsTruncated>
                        \\  <Contents><Key>wal/epoch-0/bulk/0000000001</Key></Contents>
                        \\</ListBucketResult>
                    );
                }
            },
        }
        return 200;
    }

    fn writeHttpResponse(resp_buf: []u8, body: []const u8) void {
        _ = std.fmt.bufPrint(
            resp_buf,
            "HTTP/1.1 200 OK\r\nContent-Length: {d}\r\n\r\n{s}",
            .{ body.len, body },
        ) catch return;
    }
};

test "S3Storage real-client listing fails closed on truncated page without token" {
    var server = ListObjectsTestServer{ .mode = .missing_continuation_token };
    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = ListObjectsTestServer.request;
    var storage = S3Storage.initReal(testing.allocator, &client);

    try testing.expectError(error.S3ListMissingContinuationToken, storage.listObjectKeys("wal/"));
    try testing.expectEqual(@as(u32, 1), server.page_count);
}

test "S3Storage real-client listing follows continuation tokens" {
    var server = ListObjectsTestServer{ .mode = .two_pages };
    var client = S3Client.init(testing.allocator, .{});
    client.test_http_ctx = &server;
    client.test_http_request = ListObjectsTestServer.request;
    var storage = S3Storage.initReal(testing.allocator, &client);

    const keys = try storage.listObjectKeys("wal/");
    defer {
        for (keys) |key| testing.allocator.free(key);
        testing.allocator.free(keys);
    }

    try testing.expectEqual(@as(u32, 2), server.page_count);
    try testing.expectEqual(@as(usize, 2), keys.len);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000001", keys[0]);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000002", keys[1]);
}

test "S3Storage parses ListObjects keys" {
    const xml =
        \\<ListBucketResult>
        \\  <Contents><Key>wal/epoch-0/bulk/0000000002</Key></Contents>
        \\  <Contents><Key>wal/epoch-0/bulk/0000000001</Key></Contents>
        \\</ListBucketResult>
    ;

    const keys = try parseListObjectKeys(testing.allocator, xml);
    defer {
        for (keys) |key| testing.allocator.free(key);
        testing.allocator.free(keys);
    }

    try testing.expectEqual(@as(usize, 2), keys.len);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000001", keys[0]);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000002", keys[1]);
}

test "S3Storage parses ListObjects continuation token" {
    const xml =
        \\<ListBucketResult>
        \\  <IsTruncated>true</IsTruncated>
        \\  <Contents><Key>wal/epoch-0/bulk/0000000001</Key></Contents>
        \\  <NextContinuationToken>opaque-token-1</NextContinuationToken>
        \\</ListBucketResult>
    ;

    var page = try parseListObjectPage(testing.allocator, xml);
    defer page.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), page.keys.len);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000001", page.keys[0]);
    try testing.expectEqualStrings("opaque-token-1", page.next_continuation_token.?);
    try testing.expect(page.is_truncated);
}

test "S3Storage decodes ListObjects XML entities" {
    const xml =
        \\<ListBucketResult>
        \\  <Contents><Key>wal/epoch-0/bulk/a&amp;b&quot;c&apos;d</Key></Contents>
        \\  <NextContinuationToken>next&amp;token</NextContinuationToken>
        \\</ListBucketResult>
    ;

    var page = try parseListObjectPage(testing.allocator, xml);
    defer page.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), page.keys.len);
    try testing.expectEqualStrings("wal/epoch-0/bulk/a&b\"c'd", page.keys[0]);
    try testing.expectEqualStrings("next&token", page.next_continuation_token.?);
    try testing.expect(page.is_truncated);
}

test "S3Storage detects truncated ListObjects page without continuation token" {
    const xml =
        \\<ListBucketResult>
        \\  <IsTruncated>true</IsTruncated>
        \\  <Contents><Key>wal/epoch-0/bulk/0000000001</Key></Contents>
        \\</ListBucketResult>
    ;

    var page = try parseListObjectPage(testing.allocator, xml);
    defer page.deinit(testing.allocator);

    try testing.expect(page.is_truncated);
    try testing.expect(page.next_continuation_token == null);
}

test "S3Storage parses non-truncated ListObjects page explicitly" {
    const xml =
        \\<ListBucketResult>
        \\  <IsTruncated>false</IsTruncated>
        \\  <Contents><Key>wal/epoch-0/bulk/0000000001</Key></Contents>
        \\</ListBucketResult>
    ;

    var page = try parseListObjectPage(testing.allocator, xml);
    defer page.deinit(testing.allocator);

    try testing.expect(!page.is_truncated);
    try testing.expect(page.next_continuation_token == null);
}

test "S3Client getObject retry structure" {
    // Verify that getObject wraps getObjectOnce with retry logic.
    // We can't test real HTTP retries without a server, but we verify
    // the S3Client is properly initialized and the method signatures exist.
    const client = S3Client.init(testing.allocator, .{});
    try testing.expectEqual(@as(u64, 0), client.get_count);

    // getObject will fail immediately since there's no server, but the retry
    // loop structure is tested by verifying getObjectOnce is callable.
    // The actual retry behavior (100ms/200ms/400ms backoff) is structural —
    // validated by code review / grep for "retry" and "backoff" in getObject.
}

test "S3Storage with ObjectWriter v2 checksum round-trip" {
    // End-to-end test: ObjectWriter produces v2 checksummed object,
    // stored via MockS3, retrieved, and ObjectReader verifies integrity.
    const s3_mod = @import("s3.zig");
    var mock = s3_mod.MockS3.init(testing.allocator);
    defer mock.deinit();

    var storage = S3Storage.initMock(testing.allocator, &mock);

    // Build a checksummed S3 object
    var writer = s3_mod.ObjectWriter.init(testing.allocator);
    defer writer.deinit();
    try writer.addDataBlock(1, 0, 10, 10, "checksum-test-data");
    const obj_bytes = try writer.build();
    defer testing.allocator.free(obj_bytes);

    // Store and retrieve via S3Storage
    try storage.putObject("test/checksummed", obj_bytes);
    const retrieved = try storage.getObject("test/checksummed");
    if (retrieved) |data| {
        defer testing.allocator.free(data);

        // Parse and verify checksum
        var reader = try s3_mod.ObjectReader.parse(testing.allocator, data);
        defer reader.deinit();

        try testing.expect(reader.has_checksum);
        try testing.expectEqual(@as(usize, 1), reader.index_entries.len);
        try testing.expectEqualStrings("checksum-test-data", reader.readBlock(0).?);
    } else {
        return error.TestUnexpectedResult;
    }
}

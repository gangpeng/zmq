const std = @import("std");
const net = @import("net_compat");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const MetricRegistry = @import("core").MetricRegistry;
const TlsContext = @import("security").tls.TlsContext;
const OpenSslLib = @import("security").openssl.OpenSslLib;
const log = std.log.scoped(.metrics_server);

/// HTTP server for Prometheus metrics and health endpoints.
/// Listens on a separate port and serves:
/// - GET /metrics — Prometheus exposition format
/// - GET /health — liveness probe (200 if event loop is responsive)
/// - GET /ready — readiness probe (200 when broker startup is complete, 503 otherwise)
///
/// Supports optional TLS for encrypted metrics transport. When tls_context
/// is set, connections are wrapped with SSL (blocking handshake since
/// metrics is simple request/response).
pub const MetricsServer = struct {
    pub const ProbeResponse = struct {
        status: []const u8,
        content_type: []const u8,
        body: []const u8,
    };

    pub const ReadinessState = enum {
        starting,
        ready,
        shutting_down,
    };

    const Route = union(enum) {
        static: ProbeResponse,
        metrics,
    };

    port: u16,
    registry: *MetricRegistry,
    allocator: Allocator,
    listener: ?posix.socket_t = null,
    running: bool = false,
    /// Set to true once the broker has completed startup (listening, metrics registered).
    /// /ready returns 503 until this is true.
    startup_complete: bool = false,
    /// Set once graceful shutdown begins. /ready returns 503 while the broker drains.
    shutting_down: bool = false,
    // NOTE: TLS for the metrics server is available but not wired into main.zig
    // by default. To enable, create a TlsContext and pass it here. Typically
    // metrics endpoints are behind a reverse proxy that terminates TLS, so
    // direct TLS on the metrics port is rarely needed.
    tls_context: ?*TlsContext = null,

    pub fn init(alloc: Allocator, port: u16, registry: *MetricRegistry) MetricsServer {
        return .{
            .port = port,
            .registry = registry,
            .allocator = alloc,
        };
    }

    pub fn initWithTls(alloc: Allocator, port: u16, registry: *MetricRegistry, tls_ctx: ?*TlsContext) MetricsServer {
        return .{
            .port = port,
            .registry = registry,
            .allocator = alloc,
            .tls_context = tls_ctx,
        };
    }

    /// Start serving metrics. Call in a separate thread.
    pub fn serve(self: *MetricsServer) !void {
        const sock = try @import("posix_compat").socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
        defer @import("posix_compat").close(sock);

        const optval: i32 = 1;
        try posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&optval));

        const addr = try net.Address.parseIp4("0.0.0.0", self.port);
        try @import("posix_compat").bind(sock, &addr.any, addr.getOsSockLen());
        try @import("posix_compat").listen(sock, 16);

        self.listener = sock;
        self.running = true;

        while (self.running) {
            var poll_fds = [_]posix.pollfd{.{
                .fd = sock,
                .events = posix.POLL.IN,
                .revents = 0,
            }};

            const ready = try posix.poll(&poll_fds, 500);
            if (ready == 0) continue;

            var client_addr: posix.sockaddr = undefined;
            var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
            const client = @import("posix_compat").accept(sock, &client_addr, &addr_len, 0) catch continue;

            if (self.tls_context) |tls_ctx| {
                self.handleTlsClient(client, tls_ctx);
            } else {
                self.handlePlainClient(client);
            }
        }
    }

    /// Handle a plain (non-TLS) client connection.
    fn handlePlainClient(self: *MetricsServer, client: posix.socket_t) void {
        defer @import("posix_compat").close(client);

        // Read request
        var req_buf: [1024]u8 = undefined;
        const req_len = posix.read(client, &req_buf) catch return;
        if (req_len == 0) return;
        const request = req_buf[0..req_len];

        switch (self.routeRequest(request)) {
            .static => |resp| self.sendPlainResponse(client, resp.status, resp.content_type, resp.body),
            .metrics => self.servePlainMetrics(client),
        }
    }

    /// Handle a TLS-wrapped client connection.
    fn handleTlsClient(self: *MetricsServer, client: posix.socket_t, tls_ctx: *TlsContext) void {
        const ossl = tls_ctx.getOpenSsl() orelse {
            @import("posix_compat").close(client);
            return;
        };

        // Create SSL object and bind to fd
        const ssl = tls_ctx.createSsl(client) catch {
            @import("posix_compat").close(client);
            return;
        };
        defer {
            _ = ossl.SSL_shutdown(ssl);
            ossl.SSL_free(ssl);
            @import("posix_compat").close(client);
        }

        // Blocking SSL handshake (metrics server handles one request at a time)
        const accept_ret = ossl.SSL_accept(ssl);
        if (accept_ret != 1) {
            const err_str = ossl.getErrorString();
            log.warn("Metrics TLS handshake failed: {s}", .{std.mem.sliceTo(&err_str, 0)});
            return;
        }

        // Read request via SSL
        var req_buf: [1024]u8 = undefined;
        const ret = ossl.SSL_read(ssl, &req_buf, @intCast(req_buf.len));
        if (ret <= 0) return;
        const request = req_buf[0..@intCast(ret)];

        switch (self.routeRequest(request)) {
            .static => |resp| self.sendSslResponse(ossl, ssl, resp.status, resp.content_type, resp.body),
            .metrics => self.serveSslMetrics(ossl, ssl),
        }
    }

    fn routeRequest(self: *const MetricsServer, request: []const u8) Route {
        const target = requestTarget(request) orelse {
            return .{ .static = notFoundResponse() };
        };

        if (std.mem.eql(u8, target, "/health")) {
            return .{ .static = healthResponse() };
        }
        if (std.mem.eql(u8, target, "/ready")) {
            return .{ .static = self.readinessResponse() };
        }
        if (std.mem.eql(u8, target, "/metrics")) {
            return .metrics;
        }
        return .{ .static = notFoundResponse() };
    }

    fn requestTarget(request: []const u8) ?[]const u8 {
        if (!std.mem.startsWith(u8, request, "GET ")) return null;
        const target_start: usize = 4;
        const target_tail = request[target_start..];
        const target_len = std.mem.indexOfScalar(u8, target_tail, ' ') orelse return null;
        if (target_len == 0) return null;
        return target_tail[0..target_len];
    }

    fn healthResponse() ProbeResponse {
        return .{
            .status = "200 OK",
            .content_type = "text/plain",
            .body = "OK\n",
        };
    }

    fn notFoundResponse() ProbeResponse {
        return .{
            .status = "404 Not Found",
            .content_type = "text/plain",
            .body = "Not Found\n",
        };
    }

    pub fn readinessState(self: *const MetricsServer) ReadinessState {
        if (self.shutting_down) return .shutting_down;
        if (!self.startup_complete) return .starting;
        return .ready;
    }

    pub fn readinessResponse(self: *const MetricsServer) ProbeResponse {
        return switch (self.readinessState()) {
            .ready => .{
                .status = "200 OK",
                .content_type = "text/plain",
                .body = "READY\n",
            },
            .starting, .shutting_down => .{
                .status = "503 Service Unavailable",
                .content_type = "text/plain",
                .body = "NOT READY\n",
            },
        };
    }

    fn servePlainMetrics(self: *MetricsServer, client: posix.socket_t) void {
        const metrics_body = self.registry.exportPrometheus(self.allocator) catch return;
        defer self.allocator.free(metrics_body);

        var resp_buf = std.array_list.Managed(u8).init(self.allocator);
        defer resp_buf.deinit();
        const writer = @import("list_compat").writer(&resp_buf);
        writer.print("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{metrics_body.len}) catch return;
        writer.writeAll(metrics_body) catch return;

        _ = @import("posix_compat").write(client, resp_buf.items) catch {};
    }

    fn serveSslMetrics(self: *MetricsServer, ossl: *OpenSslLib, ssl: *anyopaque) void {
        const metrics_body = self.registry.exportPrometheus(self.allocator) catch return;
        defer self.allocator.free(metrics_body);

        var resp_buf = std.array_list.Managed(u8).init(self.allocator);
        defer resp_buf.deinit();
        const writer = @import("list_compat").writer(&resp_buf);
        writer.print("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{metrics_body.len}) catch return;
        writer.writeAll(metrics_body) catch return;

        _ = ossl.SSL_write(ssl, resp_buf.items.ptr, @intCast(resp_buf.items.len));
    }

    fn sendPlainResponse(self: *MetricsServer, client: posix.socket_t, status: []const u8, content_type: []const u8, body: []const u8) void {
        _ = self;
        var buf: [512]u8 = undefined;
        const resp = std.fmt.bufPrint(&buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ status, content_type, body.len, body }) catch return;
        _ = @import("posix_compat").write(client, resp) catch {};
    }

    fn sendSslResponse(_: *MetricsServer, ossl: *OpenSslLib, ssl: *anyopaque, status: []const u8, content_type: []const u8, body: []const u8) void {
        var buf: [512]u8 = undefined;
        const resp = std.fmt.bufPrint(&buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ status, content_type, body.len, body }) catch return;
        _ = ossl.SSL_write(ssl, resp.ptr, @intCast(resp.len));
    }

    pub fn stop(self: *MetricsServer) void {
        self.shutting_down = true;
        self.running = false;
    }

    pub fn markStartupComplete(self: *MetricsServer) void {
        self.startup_complete = true;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "MetricsServer startup_complete defaults to false" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    const server = MetricsServer.init(testing.allocator, 19090, &registry);
    try testing.expect(!server.startup_complete);
    try testing.expect(!server.shutting_down);
    try testing.expectEqual(MetricsServer.ReadinessState.starting, server.readinessState());
}

test "MetricsServer ready returns 503 when startup not complete" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    var server = MetricsServer.init(testing.allocator, 19090, &registry);
    const resp = server.readinessResponse();
    try testing.expectEqualStrings("503 Service Unavailable", resp.status);
    try testing.expectEqualStrings("text/plain", resp.content_type);
    try testing.expectEqualStrings("NOT READY\n", resp.body);
}

test "MetricsServer ready transitions from 503 to 200" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    var server = MetricsServer.init(testing.allocator, 19090, &registry);

    try testing.expectEqual(MetricsServer.ReadinessState.starting, server.readinessState());

    server.markStartupComplete();
    try testing.expectEqual(MetricsServer.ReadinessState.ready, server.readinessState());
    try testing.expectEqualStrings("200 OK", server.readinessResponse().status);
    try testing.expectEqualStrings("READY\n", server.readinessResponse().body);

    server.stop();
    try testing.expectEqual(MetricsServer.ReadinessState.shutting_down, server.readinessState());
    try testing.expectEqualStrings("503 Service Unavailable", server.readinessResponse().status);
    try testing.expectEqualStrings("NOT READY\n", server.readinessResponse().body);
}

test "MetricsServer routes fixed probes exactly" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    var server = MetricsServer.init(testing.allocator, 19090, &registry);
    server.markStartupComplete();

    const health = server.routeRequest("GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n").static;
    try testing.expectEqualStrings("200 OK", health.status);
    try testing.expectEqualStrings("OK\n", health.body);

    const ready = server.routeRequest("GET /ready HTTP/1.1\r\n\r\n").static;
    try testing.expectEqualStrings("200 OK", ready.status);
    try testing.expectEqualStrings("READY\n", ready.body);

    const ready_suffix = server.routeRequest("GET /readyz HTTP/1.1\r\n\r\n").static;
    try testing.expectEqualStrings("404 Not Found", ready_suffix.status);

    const bad_method = server.routeRequest("POST /ready HTTP/1.1\r\n\r\n").static;
    try testing.expectEqualStrings("404 Not Found", bad_method.status);
}

test "MetricsServer routes metrics through dynamic exporter" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    const server = MetricsServer.init(testing.allocator, 19090, &registry);
    switch (server.routeRequest("GET /metrics HTTP/1.1\r\n\r\n")) {
        .metrics => {},
        .static => return error.ExpectedMetricsRoute,
    }
}

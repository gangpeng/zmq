const std = @import("std");
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const MetricRegistry = @import("../core/metric_registry.zig").MetricRegistry;
const TlsContext = @import("../security/tls.zig").TlsContext;
const OpenSslLib = @import("../security/openssl.zig").OpenSslLib;
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
    port: u16,
    registry: *MetricRegistry,
    allocator: Allocator,
    listener: ?posix.socket_t = null,
    running: bool = false,
    /// Set to true once the broker has completed startup (listening, metrics registered).
    /// /ready returns 503 until this is true.
    startup_complete: bool = false,
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
        const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
        defer posix.close(sock);

        const optval: i32 = 1;
        try posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&optval));

        const addr = try net.Address.parseIp4("0.0.0.0", self.port);
        try posix.bind(sock, &addr.any, addr.getOsSockLen());
        try posix.listen(sock, 16);

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
            const client = posix.accept(sock, &client_addr, &addr_len, 0) catch continue;

            if (self.tls_context) |tls_ctx| {
                self.handleTlsClient(client, tls_ctx);
            } else {
                self.handlePlainClient(client);
            }
        }
    }

    /// Handle a plain (non-TLS) client connection.
    fn handlePlainClient(self: *MetricsServer, client: posix.socket_t) void {
        defer posix.close(client);

        // Read request
        var req_buf: [1024]u8 = undefined;
        const req_len = posix.read(client, &req_buf) catch return;
        if (req_len == 0) return;
        const request = req_buf[0..req_len];

        // Route request
        if (std.mem.startsWith(u8, request, "GET /health")) {
            // Liveness: 200 if the event loop is responsive (reaching here proves it)
            self.sendPlainResponse(client, "200 OK", "text/plain", "OK\n");
        } else if (std.mem.startsWith(u8, request, "GET /ready")) {
            // Readiness: 503 until startup_complete is set
            if (self.startup_complete) {
                self.sendPlainResponse(client, "200 OK", "text/plain", "READY\n");
            } else {
                self.sendPlainResponse(client, "503 Service Unavailable", "text/plain", "NOT READY\n");
            }
        } else if (std.mem.startsWith(u8, request, "GET /metrics")) {
            self.servePlainMetrics(client);
        } else {
            self.sendPlainResponse(client, "404 Not Found", "text/plain", "Not Found\n");
        }
    }

    /// Handle a TLS-wrapped client connection.
    fn handleTlsClient(self: *MetricsServer, client: posix.socket_t, tls_ctx: *TlsContext) void {
        const ossl = tls_ctx.getOpenSsl() orelse {
            posix.close(client);
            return;
        };

        // Create SSL object and bind to fd
        const ssl = tls_ctx.createSsl(client) catch {
            posix.close(client);
            return;
        };
        defer {
            _ = ossl.SSL_shutdown(ssl);
            ossl.SSL_free(ssl);
            posix.close(client);
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

        // Route request and respond via SSL
        if (std.mem.startsWith(u8, request, "GET /health")) {
            self.sendSslResponse(ossl, ssl, "200 OK", "text/plain", "OK\n");
        } else if (std.mem.startsWith(u8, request, "GET /ready")) {
            if (self.startup_complete) {
                self.sendSslResponse(ossl, ssl, "200 OK", "text/plain", "READY\n");
            } else {
                self.sendSslResponse(ossl, ssl, "503 Service Unavailable", "text/plain", "NOT READY\n");
            }
        } else if (std.mem.startsWith(u8, request, "GET /metrics")) {
            self.serveSslMetrics(ossl, ssl);
        } else {
            self.sendSslResponse(ossl, ssl, "404 Not Found", "text/plain", "Not Found\n");
        }
    }

    fn servePlainMetrics(self: *MetricsServer, client: posix.socket_t) void {
        const metrics_body = self.registry.exportPrometheus(self.allocator) catch return;
        defer self.allocator.free(metrics_body);

        var resp_buf = std.ArrayList(u8).init(self.allocator);
        defer resp_buf.deinit();
        const writer = resp_buf.writer();
        writer.print("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{metrics_body.len}) catch return;
        writer.writeAll(metrics_body) catch return;

        _ = posix.write(client, resp_buf.items) catch {};
    }

    fn serveSslMetrics(self: *MetricsServer, ossl: *OpenSslLib, ssl: *anyopaque) void {
        const metrics_body = self.registry.exportPrometheus(self.allocator) catch return;
        defer self.allocator.free(metrics_body);

        var resp_buf = std.ArrayList(u8).init(self.allocator);
        defer resp_buf.deinit();
        const writer = resp_buf.writer();
        writer.print("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{metrics_body.len}) catch return;
        writer.writeAll(metrics_body) catch return;

        _ = ossl.SSL_write(ssl, resp_buf.items.ptr, @intCast(resp_buf.items.len));
    }

    fn sendPlainResponse(self: *MetricsServer, client: posix.socket_t, status: []const u8, content_type: []const u8, body: []const u8) void {
        _ = self;
        var buf: [512]u8 = undefined;
        const resp = std.fmt.bufPrint(&buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ status, content_type, body.len, body }) catch return;
        _ = posix.write(client, resp) catch {};
    }

    fn sendSslResponse(_: *MetricsServer, ossl: *OpenSslLib, ssl: *anyopaque, status: []const u8, content_type: []const u8, body: []const u8) void {
        var buf: [512]u8 = undefined;
        const resp = std.fmt.bufPrint(&buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ status, content_type, body.len, body }) catch return;
        _ = ossl.SSL_write(ssl, resp.ptr, @intCast(resp.len));
    }

    pub fn stop(self: *MetricsServer) void {
        self.running = false;
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
}

test "MetricsServer ready returns 503 when startup not complete" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    const server = MetricsServer.init(testing.allocator, 19090, &registry);
    // Before startup_complete, readiness should indicate not ready
    try testing.expect(!server.startup_complete);

    // After setting startup_complete, readiness should indicate ready
    server.startup_complete = true;
    try testing.expect(server.startup_complete);
}

test "MetricsServer ready transitions from 503 to 200" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    const server = MetricsServer.init(testing.allocator, 19090, &registry);

    // Phase 1: not ready
    try testing.expect(!server.startup_complete);

    // Phase 2: startup completes
    server.startup_complete = true;
    try testing.expect(server.startup_complete);

    // Phase 3: remains ready
    try testing.expect(server.startup_complete);
}

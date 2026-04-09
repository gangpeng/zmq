const std = @import("std");
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const MetricRegistry = @import("../core/metric_registry.zig").MetricRegistry;

/// HTTP server for Prometheus metrics and health endpoints.
/// Listens on a separate port and serves:
/// - GET /metrics — Prometheus exposition format
/// - GET /health — liveness probe (always 200)
/// - GET /ready — readiness probe (200 when broker is serving)
pub const MetricsServer = struct {
    port: u16,
    registry: *MetricRegistry,
    allocator: Allocator,
    listener: ?posix.socket_t = null,
    running: bool = false,

    pub fn init(alloc: Allocator, port: u16, registry: *MetricRegistry) MetricsServer {
        return .{
            .port = port,
            .registry = registry,
            .allocator = alloc,
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
            defer posix.close(client);

            // Read request
            var req_buf: [1024]u8 = undefined;
            const req_len = posix.read(client, &req_buf) catch continue;
            if (req_len == 0) continue;
            const request = req_buf[0..req_len];

            // Route request
            if (std.mem.startsWith(u8, request, "GET /health")) {
                self.sendResponse(client, "200 OK", "text/plain", "OK\n");
            } else if (std.mem.startsWith(u8, request, "GET /ready")) {
                self.sendResponse(client, "200 OK", "text/plain", "READY\n");
            } else if (std.mem.startsWith(u8, request, "GET /metrics")) {
                self.serveMetrics(client);
            } else {
                self.sendResponse(client, "404 Not Found", "text/plain", "Not Found\n");
            }
        }
    }

    fn serveMetrics(self: *MetricsServer, client: posix.socket_t) void {
        const metrics_body = self.registry.exportPrometheus(self.allocator) catch return;
        defer self.allocator.free(metrics_body);

        var resp_buf = std.ArrayList(u8).init(self.allocator);
        defer resp_buf.deinit();
        const writer = resp_buf.writer();
        writer.print("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{metrics_body.len}) catch return;
        writer.writeAll(metrics_body) catch return;

        _ = posix.write(client, resp_buf.items) catch {};
    }

    fn sendResponse(self: *MetricsServer, client: posix.socket_t, status: []const u8, content_type: []const u8, body: []const u8) void {
        _ = self;
        var buf: [512]u8 = undefined;
        const resp = std.fmt.bufPrint(&buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ status, content_type, body.len, body }) catch return;
        _ = posix.write(client, resp) catch {};
    }

    pub fn stop(self: *MetricsServer) void {
        self.running = false;
    }
};

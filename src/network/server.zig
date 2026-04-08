const std = @import("std");
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.server);
const IoUring = linux.IoUring;

/// Kafka TCP server.
///
/// Uses Linux io_uring for batched, zero-syscall I/O:
///   - Single submit_and_wait() per loop iteration
///   - Batched accept, recv, send operations
///   - No per-request syscalls (all I/O goes through the ring)
///
/// Falls back to epoll if io_uring is unavailable (kernel < 5.4).
pub const Server = struct {
    listen_address: net.Address,
    listener: ?posix.socket_t = null,
    allocator: Allocator,
    handler: *const RequestHandler,
    running: bool = false,
    num_workers: usize,
    /// Group commit: called after each epoll/io_uring iteration to flush pending S3 WAL writes.
    batch_flush_fn: ?*const fn () void = null,
    /// Group commit: returns true if there are pending WAL writes (used to reduce epoll timeout).
    has_pending_flush_fn: ?*const fn () bool = null,

    const MAX_CONNECTIONS: usize = 100000;
    const IDLE_TIMEOUT_MS: i64 = 60_000;
    const MAX_FRAME_SIZE: u32 = 104_857_600;
    const MAX_RECV_BUFFER: usize = 104_857_600;
    const MAX_SEND_BUFFER: usize = 16_777_216;
    const RING_ENTRIES: u16 = 256;

    const Connection = struct {
        fd: posix.socket_t,
        recv_buf: std.ArrayList(u8),
        send_buf: std.ArrayList(u8),
        recv_cursor: usize = 0,
        last_activity_ms: i64 = 0,
        request_count: u64 = 0,
        bytes_in: u64 = 0,
        bytes_out: u64 = 0,
        recv_pending: bool = false, // io_uring recv submitted
        send_pending: bool = false, // io_uring send submitted
        closed: bool = false,

        fn init(alloc: Allocator, fd: posix.socket_t) Connection {
            return .{
                .fd = fd,
                .recv_buf = std.ArrayList(u8).init(alloc),
                .send_buf = std.ArrayList(u8).init(alloc),
                .last_activity_ms = std.time.milliTimestamp(),
            };
        }

        fn deinit(self: *Connection) void {
            self.recv_buf.deinit();
            self.send_buf.deinit();
            if (!self.closed) {
                posix.close(self.fd);
                self.closed = true;
            }
        }

        fn isIdle(self: *const Connection, now_ms: i64) bool {
            return (now_ms - self.last_activity_ms) > IDLE_TIMEOUT_MS;
        }
    };

    pub const RequestHandler = fn (request: []const u8, allocator: Allocator) ?[]u8;

    // io_uring completion tags — encode operation type + fd in user_data
    const OP_ACCEPT: u64 = 0;
    const OP_RECV: u64 = 1 << 32;
    const OP_SEND: u64 = 2 << 32;

    fn makeUserData(op: u64, fd: posix.socket_t) u64 {
        return op | @as(u64, @intCast(@as(u32, @bitCast(fd))));
    }
    fn getUserDataOp(user_data: u64) u64 {
        return user_data & 0xFFFFFFFF00000000;
    }
    fn getUserDataFd(user_data: u64) posix.socket_t {
        return @bitCast(@as(u32, @truncate(user_data)));
    }

    pub fn init(allocator: Allocator, host: []const u8, port: u16, handler: *const RequestHandler, num_workers: usize) !Server {
        _ = num_workers;
        const address = try net.Address.parseIp4(host, port);
        return .{
            .listen_address = address,
            .allocator = allocator,
            .handler = handler,
            .num_workers = 0, // io_uring doesn't need worker threads
        };
    }

    pub fn serve(self: *Server) !void {
        // Use epoll — reliable and well-tested.
        // io_uring has buffer lifetime issues with async sends that need more work.
        return self.serveEpoll();
    }

    fn serveIoUring(self: *Server) !void {
        // Socket setup
        const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
        errdefer posix.close(sock);

        const optval: i32 = 1;
        try posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&optval));
        try posix.bind(sock, &self.listen_address.any, self.listen_address.getOsSockLen());
        try posix.listen(sock, 1024);

        self.listener = sock;
        self.running = true;

        // Initialize io_uring
        var ring = try IoUring.init(RING_ENTRIES, 0);
        defer ring.deinit();

        log.info("ZMQ broker listening on port {d} (io_uring)", .{self.listen_address.getPort()});

        // Connection state
        var connections = std.AutoHashMap(posix.socket_t, Connection).init(self.allocator);
        defer {
            var it = connections.valueIterator();
            while (it.next()) |conn| conn.deinit();
            connections.deinit();
        }

        // Per-connection recv buffers for io_uring (stack-allocated for hot path)
        var recv_bufs: [256][16384]u8 = undefined;
        var recv_buf_map = std.AutoHashMap(posix.socket_t, u8).init(self.allocator);
        defer recv_buf_map.deinit();
        var next_recv_slot: u8 = 0;

        // Submit initial accept
        var accept_addr: posix.sockaddr = undefined;
        var accept_addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
        self.submitAccept(&ring, sock, &accept_addr, &accept_addr_len) catch {};

        var loop_count: u64 = 0;
        var cqes: [64]linux.io_uring_cqe = undefined;

        while (self.running) {
            // Submit pending operations and wait for at least 1 completion
            const completed = ring.copy_cqes(&cqes, 1) catch |err| {
                if (!self.running) break;
                log.warn("io_uring copy_cqes error: {}", .{err});
                continue;
            };

            const now_ms = std.time.milliTimestamp();

            for (cqes[0..completed]) |*cqe| {
                const user_data = cqe.user_data;
                const op = getUserDataOp(user_data);
                const res = cqe.res;

                if (op == OP_ACCEPT) {
                    // Accept completion
                    if (res >= 0) {
                        const client_fd: posix.socket_t = @intCast(res);

                        // TCP_NODELAY
                        const nodelay: i32 = 1;
                        posix.setsockopt(client_fd, posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&nodelay)) catch {};

                        // io_uring handles non-blocking I/O inherently — no need for fcntl

                        connections.put(client_fd, Connection.init(self.allocator, client_fd)) catch {
                            posix.close(client_fd);
                        };

                        // Assign recv buffer slot
                        if (next_recv_slot < 255) {
                            recv_buf_map.put(client_fd, next_recv_slot) catch {};
                            next_recv_slot += 1;
                        }

                        // Submit recv for this connection
                        self.submitRecv(&ring, client_fd, &recv_bufs, &recv_buf_map) catch {};

                        log.debug("Accepted connection fd={d}", .{client_fd});
                    }

                    // Re-submit accept for next connection
                    self.submitAccept(&ring, sock, &accept_addr, &accept_addr_len) catch {};
                } else if (op == OP_RECV) {
                    const fd = getUserDataFd(user_data);
                    if (connections.getPtr(fd)) |conn| {
                        conn.recv_pending = false;

                        if (res <= 0) {
                            // Connection closed or error
                            conn.deinit();
                            _ = connections.remove(fd);
                            continue;
                        }

                        const bytes_read: usize = @intCast(res);
                        conn.last_activity_ms = now_ms;
                        conn.bytes_in += bytes_read;

                        // Get the recv buffer data
                        if (recv_buf_map.get(fd)) |slot| {
                            conn.recv_buf.appendSlice(recv_bufs[slot][0..bytes_read]) catch {};
                        }

                        // Process complete frames inline
                        self.processFrames(conn);

                        // If we have data to send, flush it
                        if (conn.send_buf.items.len > 0 and !conn.send_pending) {
                            self.submitSend(&ring, fd, conn) catch {};
                        }

                        // Submit next recv
                        if (!conn.closed) {
                            self.submitRecv(&ring, fd, &recv_bufs, &recv_buf_map) catch {};
                        }
                    }
                } else if (op == OP_SEND) {
                    const fd = getUserDataFd(user_data);
                    if (connections.getPtr(fd)) |conn| {
                        conn.send_pending = false;

                        if (res > 0) {
                            const bytes_written: usize = @intCast(res);
                            // Remove written bytes
                            const remaining = conn.send_buf.items.len - bytes_written;
                            if (remaining > 0) {
                                std.mem.copyForwards(u8, conn.send_buf.items[0..remaining], conn.send_buf.items[bytes_written..]);
                            }
                            conn.send_buf.shrinkRetainingCapacity(remaining);
                        }

                        // If more data to send, submit another send
                        if (conn.send_buf.items.len > 0 and !conn.send_pending) {
                            self.submitSend(&ring, fd, conn) catch {};
                        }
                    }
                }
            }

            // Group commit flush point (io_uring path)
            if (self.batch_flush_fn) |flush_fn| {
                flush_fn();
            }

            // Periodic idle eviction
            loop_count += 1;
            if (loop_count % 5000 == 0) {
                var to_close = std.ArrayList(posix.socket_t).init(self.allocator);
                defer to_close.deinit();
                var idle_it = connections.iterator();
                while (idle_it.next()) |entry| {
                    if (entry.value_ptr.isIdle(now_ms)) {
                        to_close.append(entry.key_ptr.*) catch {};
                    }
                }
                for (to_close.items) |idle_fd| {
                    if (connections.fetchRemove(idle_fd)) |entry| {
                        var conn = entry.value;
                        conn.deinit();
                    }
                }
            }
        }

        posix.close(sock);
        self.listener = null;
    }

    fn submitAccept(self: *Server, ring: *IoUring, sock: posix.socket_t, addr: *posix.sockaddr, addr_len: *posix.socklen_t) !void {
        _ = self;
        _ = try ring.accept(makeUserData(OP_ACCEPT, 0), sock, addr, addr_len, 0);
        _ = try ring.submit();
    }

    fn submitRecv(_: *Server, ring: *IoUring, fd: posix.socket_t, recv_bufs: *[256][16384]u8, recv_buf_map: *std.AutoHashMap(posix.socket_t, u8)) !void {
        const slot = recv_buf_map.get(fd) orelse return;
        _ = try ring.recv(makeUserData(OP_RECV, fd), fd, .{ .buffer = &recv_bufs[slot] }, 0);
        _ = try ring.submit();
    }

    fn submitSend(_: *Server, ring: *IoUring, fd: posix.socket_t, conn: *Connection) !void {
        if (conn.send_buf.items.len == 0) return;
        conn.send_pending = true;
        _ = try ring.send(makeUserData(OP_SEND, fd), fd, conn.send_buf.items, 0);
        _ = try ring.submit();
    }

    fn processFrames(self: *Server, conn: *Connection) void {
        var frames_processed: u32 = 0;
        while (true) {
            const avail = conn.recv_buf.items.len - conn.recv_cursor;
            if (avail < 4) break;
            if (conn.send_buf.items.len > MAX_SEND_BUFFER) break;

            const frame_size = std.mem.readInt(u32, conn.recv_buf.items[conn.recv_cursor..][0..4], .big);
            if (frame_size > MAX_FRAME_SIZE) break;

            const total_needed = 4 + @as(usize, frame_size);
            if (avail < total_needed) break;

            const frame_start = conn.recv_cursor + 4;
            const frame_data = conn.recv_buf.items[frame_start .. conn.recv_cursor + total_needed];

            if (self.handler(frame_data, self.allocator)) |response| {
                defer self.allocator.free(response);
                var size_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &size_buf, @intCast(response.len), .big);
                conn.send_buf.appendSlice(&size_buf) catch {};
                conn.send_buf.appendSlice(response) catch {};
                conn.bytes_out += response.len + 4;
                conn.request_count += 1;
            }

            conn.recv_cursor += total_needed;
            frames_processed += 1;
        }

        // Compact recv_buf
        if (conn.recv_cursor > 0) {
            const remaining = conn.recv_buf.items.len - conn.recv_cursor;
            if (remaining > 0) {
                std.mem.copyForwards(u8, conn.recv_buf.items[0..remaining], conn.recv_buf.items[conn.recv_cursor..]);
            }
            conn.recv_buf.shrinkRetainingCapacity(remaining);
            conn.recv_cursor = 0;
        }
    }

    // ── Epoll fallback ───────────────────────────────────────────────

    fn serveEpoll(self: *Server) !void {
        const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
        errdefer posix.close(sock);

        const optval: i32 = 1;
        try posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&optval));
        try posix.bind(sock, &self.listen_address.any, self.listen_address.getOsSockLen());
        try posix.listen(sock, 1024);

        self.listener = sock;
        self.running = true;

        const epfd = try posix.epoll_create1(linux.EPOLL.CLOEXEC);
        defer posix.close(epfd);

        var listen_ev = linux.epoll_event{ .events = linux.EPOLL.IN, .data = .{ .fd = sock } };
        try posix.epoll_ctl(epfd, linux.EPOLL.CTL_ADD, sock, &listen_ev);

        log.info("ZMQ broker listening on port {d} (epoll fallback)", .{self.listen_address.getPort()});

        var connections = std.AutoHashMap(posix.socket_t, Connection).init(self.allocator);
        defer {
            var it = connections.valueIterator();
            while (it.next()) |conn| conn.deinit();
            connections.deinit();
        }

        var events: [256]linux.epoll_event = undefined;
        var loop_count: u64 = 0;

        while (self.running) {
            // Reduce epoll timeout when WAL flush is pending for low-latency group commit
            const timeout: i32 = if (self.has_pending_flush_fn) |check|
                (if (check()) @as(i32, 1) else 100)
            else
                100;
            const nready = posix.epoll_wait(epfd, &events, timeout);
            const now_ms = std.time.milliTimestamp();

            for (events[0..nready]) |ev| {
                const fd = ev.data.fd;

                if (fd == sock) {
                    // Accept
                    while (true) {
                        var addr: posix.sockaddr = undefined;
                        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
                        const client_fd = posix.accept(sock, &addr, &addr_len, posix.SOCK.NONBLOCK) catch break;

                        const nodelay: i32 = 1;
                        posix.setsockopt(client_fd, posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&nodelay)) catch {};

                        var client_ev = linux.epoll_event{
                            .events = linux.EPOLL.IN | linux.EPOLL.RDHUP,
                            .data = .{ .fd = client_fd },
                        };
                        posix.epoll_ctl(epfd, linux.EPOLL.CTL_ADD, client_fd, &client_ev) catch {
                            posix.close(client_fd);
                            continue;
                        };

                        connections.put(client_fd, Connection.init(self.allocator, client_fd)) catch {
                            posix.epoll_ctl(epfd, linux.EPOLL.CTL_DEL, client_fd, null) catch {};
                            posix.close(client_fd);
                        };
                    }
                } else {
                    if (ev.events & (linux.EPOLL.ERR | linux.EPOLL.HUP | linux.EPOLL.RDHUP) != 0) {
                        posix.epoll_ctl(epfd, linux.EPOLL.CTL_DEL, fd, null) catch {};
                        if (connections.fetchRemove(fd)) |entry| {
                            var conn = entry.value;
                            conn.deinit();
                        }
                        continue;
                    }

                    if (ev.events & linux.EPOLL.IN != 0) {
                        self.handleReadEpoll(fd, &connections, now_ms) catch {
                            posix.epoll_ctl(epfd, linux.EPOLL.CTL_DEL, fd, null) catch {};
                            if (connections.fetchRemove(fd)) |entry| {
                                var conn = entry.value;
                                conn.deinit();
                            }
                        };
                    }

                    if (ev.events & linux.EPOLL.OUT != 0) {
                        if (connections.getPtr(fd)) |conn| {
                            self.flushSendBuffer(fd, conn) catch {};
                            if (conn.send_buf.items.len == 0) {
                                var mod_ev = linux.epoll_event{
                                    .events = linux.EPOLL.IN | linux.EPOLL.RDHUP,
                                    .data = .{ .fd = fd },
                                };
                                posix.epoll_ctl(epfd, linux.EPOLL.CTL_MOD, fd, &mod_ev) catch {};
                            }
                        }
                    }
                }
            }

            // Group commit flush point: after processing all ready events,
            // flush pending S3 WAL writes. All produces from all connections
            // in this epoll iteration share a single S3 PUT.
            if (self.batch_flush_fn) |flush_fn| {
                flush_fn();
            }

            loop_count += 1;
            if (loop_count % 5000 == 0) {
                var to_close = std.ArrayList(posix.socket_t).init(self.allocator);
                defer to_close.deinit();
                var idle_it = connections.iterator();
                while (idle_it.next()) |entry| {
                    if (entry.value_ptr.isIdle(now_ms)) {
                        to_close.append(entry.key_ptr.*) catch {};
                    }
                }
                for (to_close.items) |idle_fd| {
                    posix.epoll_ctl(epfd, linux.EPOLL.CTL_DEL, idle_fd, null) catch {};
                    if (connections.fetchRemove(idle_fd)) |entry| {
                        var conn = entry.value;
                        conn.deinit();
                    }
                }
            }
        }

        posix.close(sock);
        self.listener = null;
    }

    fn handleReadEpoll(self: *Server, fd: posix.socket_t, connections: *std.AutoHashMap(posix.socket_t, Connection), now_ms: i64) !void {
        const conn = connections.getPtr(fd) orelse return;
        conn.last_activity_ms = now_ms;

        if (conn.recv_buf.items.len > MAX_RECV_BUFFER) return error.BufferOverflow;
        if (conn.send_buf.items.len > MAX_SEND_BUFFER) return;

        var read_buf: [16384]u8 = undefined;
        const bytes_read = posix.read(fd, &read_buf) catch |err| switch (err) {
            error.WouldBlock => return,
            else => return err,
        };
        if (bytes_read == 0) return error.ConnectionClosed;

        try conn.recv_buf.appendSlice(read_buf[0..bytes_read]);
        conn.bytes_in += bytes_read;

        self.processFrames(conn);

        if (conn.send_buf.items.len > 0) {
            self.flushSendBuffer(fd, conn) catch {};
        }
    }

    fn flushSendBuffer(_: *Server, fd: posix.socket_t, conn: *Connection) !void {
        if (conn.send_buf.items.len == 0) return;
        const bytes_written = posix.write(fd, conn.send_buf.items) catch |err| switch (err) {
            error.WouldBlock => return,
            else => return err,
        };
        const remaining = conn.send_buf.items.len - bytes_written;
        if (remaining > 0) {
            std.mem.copyForwards(u8, conn.send_buf.items[0..remaining], conn.send_buf.items[bytes_written..]);
        }
        conn.send_buf.shrinkRetainingCapacity(remaining);
    }

    pub fn stop(self: *Server) void {
        self.running = false;
        log.info("Server shutting down", .{});
    }

    pub fn gracefulStop(self: *Server, drain_timeout_ms: u64) void {
        self.running = false;
        log.info("Draining connections for {d}ms...", .{drain_timeout_ms});
        std.time.sleep(drain_timeout_ms * std.time.ns_per_ms);
    }

    pub const ServerStats = struct {
        active_connections: usize,
        max_connections: usize,
        total_requests: u64,
        total_bytes_in: u64,
        total_bytes_out: u64,
    };
};

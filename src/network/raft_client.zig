const std = @import("std");
const posix = std.posix;
const log = std.log.scoped(.raft_client);
const Allocator = std.mem.Allocator;
const net = @import("net_compat");
const protocol = @import("protocol");
const ser = protocol.serialization;
const header_mod = protocol.header;
const generated = protocol.generated;
const OpenSslLib = @import("security").openssl.OpenSslLib;
const TlsClientContext = @import("security").tls.TlsClientContext;

/// RPC client for sending Raft protocol messages to other brokers.
/// Each RaftClient represents a connection to a single peer broker.
///
/// Used for:
/// - Sending Vote requests during elections (API 52)
/// - Sending BeginQuorumEpoch heartbeats (API 53)
/// - Sending EndQuorumEpoch on leader step-down (API 54)
pub const RaftClient = struct {
    host: []const u8,
    port: u16,
    peer_id: i32,
    fd: ?posix.fd_t = null,
    allocator: Allocator,
    next_correlation_id: i32 = 1,
    /// SSL object for this connection (non-null when TLS handshake succeeded)
    ssl_ptr: ?*anyopaque = null,
    /// Reference to loaded OpenSSL library (for SSL_read/SSL_write/SSL_shutdown)
    openssl: ?*const OpenSslLib = null,
    /// Shared client TLS context for creating SSL objects on connect
    tls_ctx: ?*TlsClientContext = null,

    pub fn init(alloc: Allocator, peer_id: i32, host: []const u8, port: u16) RaftClient {
        return .{
            .host = host,
            .port = port,
            .peer_id = peer_id,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *RaftClient) void {
        self.disconnect();
    }

    pub fn disconnect(self: *RaftClient) void {
        // TLS shutdown before closing the socket
        if (self.ssl_ptr) |ssl| {
            if (self.openssl) |ossl| {
                _ = ossl.SSL_shutdown(ssl);
                ossl.SSL_free(ssl);
            }
            self.ssl_ptr = null;
            self.openssl = null;
        }
        if (self.fd) |fd| {
            @import("posix_compat").close(fd);
            self.fd = null;
        }
    }

    /// Ensure we have an active TCP connection to the peer.
    /// Supports both numeric IPs ("127.0.0.1") and hostnames ("node0") via DNS resolution.
    fn ensureConnected(self: *RaftClient) !void {
        if (self.fd != null) return;

        // Try numeric IP first, fall back to DNS resolution for hostnames (e.g. "node0" in Docker)
        const addr = net.Address.parseIp4(self.host, self.port) catch blk: {
            const addr_list = net.getAddressList(self.allocator, self.host, self.port) catch |err| {
                log.warn("DNS resolution failed for {s}:{d}: {}", .{ self.host, self.port, err });
                return error.ConnectionRefused;
            };
            defer addr_list.deinit();
            if (addr_list.addrs.len == 0) {
                log.warn("No addresses found for {s}:{d}", .{ self.host, self.port });
                return error.ConnectionRefused;
            }
            break :blk addr_list.addrs[0];
        };
        const fd = try @import("posix_compat").socket(addr.any.family, posix.SOCK.STREAM, 0);

        @import("posix_compat").connect(fd, &addr.any, addr.getOsSockLen()) catch |err| {
            @import("posix_compat").close(fd);
            return err;
        };

        self.fd = fd;
        log.info("Connected to peer {d} at {s}:{d}", .{ self.peer_id, self.host, self.port });

        // TLS handshake if a client TLS context is configured
        if (self.tls_ctx) |tls| {
            if (tls.initialized) {
                const ssl = tls.wrapConnection(fd) catch |err| {
                    log.warn("TLS handshake to peer {d} failed: {}", .{ self.peer_id, err });
                    @import("posix_compat").close(fd);
                    self.fd = null;
                    return error.ConnectionRefused;
                };
                self.ssl_ptr = ssl;
                self.openssl = if (tls.getOpenSsl()) |ossl| ossl else null;
            }
        }
    }

    /// Send data over the connection, using SSL_write if TLS is active.
    fn sslSend(self: *RaftClient, data: []const u8) !usize {
        if (self.ssl_ptr) |ssl| {
            const ossl = self.openssl orelse return error.TlsNotInitialized;
            const len: c_int = @intCast(@min(data.len, std.math.maxInt(c_int)));
            const ret = ossl.SSL_write(ssl, data.ptr, len);
            if (ret > 0) return @intCast(ret);
            const ssl_err = ossl.SSL_get_error(ssl, ret);
            log.warn("SSL_write failed: ssl_error={d}", .{ssl_err});
            return error.WriteError;
        }
        return @import("posix_compat").send(self.fd.?, data, 0);
    }

    /// Receive data from the connection, using SSL_read if TLS is active.
    fn sslRecv(self: *RaftClient, buf: []u8) !usize {
        if (self.ssl_ptr) |ssl| {
            const ossl = self.openssl orelse return error.TlsNotInitialized;
            const len: c_int = @intCast(@min(buf.len, std.math.maxInt(c_int)));
            const ret = ossl.SSL_read(ssl, buf.ptr, len);
            if (ret > 0) return @intCast(ret);
            const ssl_err = ossl.SSL_get_error(ssl, ret);
            if (ssl_err == OpenSslLib.SSL_ERROR_ZERO_RETURN) return 0;
            log.warn("SSL_read failed: ssl_error={d}", .{ssl_err});
            return error.ReadError;
        }
        return @import("posix_compat").recv(self.fd.?, buf, 0);
    }

    /// Send a Vote request (API key 52) and get the response.
    /// Returns: (vote_granted, leader_epoch)
    pub fn sendVoteRequest(
        self: *RaftClient,
        cluster_id: []const u8,
        candidate_epoch: i32,
        candidate_id: i32,
        last_log_offset: i64,
        last_log_epoch: i32,
    ) !VoteResult {
        const Req = generated.vote_request.VoteRequest;
        const Resp = generated.vote_response.VoteResponse;

        const corr_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        // Build Vote request (API key 52, version 0) with generated KRaft
        // schema framing. Vote v0 is flexible and uses request header v2.
        const api_key: i16 = 52;
        const api_version: i16 = 0;
        var req_buf: [512]u8 = undefined;
        var pos: usize = 0;

        ser.writeI16(&req_buf, &pos, api_key);
        ser.writeI16(&req_buf, &pos, api_version);
        ser.writeI32(&req_buf, &pos, corr_id);
        ser.writeCompactString(&req_buf, &pos, "raft-client"); // client_id
        ser.writeEmptyTaggedFields(&req_buf, &pos); // header tagged fields

        const partitions = [_]Req.TopicData.PartitionData{.{
            .partition_index = 0,
            .candidate_epoch = candidate_epoch,
            .candidate_id = candidate_id,
            .last_offset_epoch = last_log_epoch,
            .last_offset = last_log_offset,
        }};
        const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
        const req = Req{
            .cluster_id = cluster_id,
            .topics = &topics,
        };
        req.serialize(&req_buf, &pos, api_version);

        const response = try self.sendRawRequest(req_buf[0..pos], self.allocator);
        defer self.allocator.free(response);

        var rpos: usize = 0;
        var resp_header = try header_mod.ResponseHeader.deserialize(
            self.allocator,
            response,
            &rpos,
            header_mod.responseHeaderVersion(api_key, api_version),
        );
        defer resp_header.deinit(self.allocator);

        var resp = try Resp.deserialize(self.allocator, response, &rpos, api_version);
        defer self.freeVoteResponse(&resp);

        if (resp.error_code != 0 or resp.topics.len == 0 or resp.topics[0].partitions.len == 0) {
            return .{ .vote_granted = false, .epoch = candidate_epoch };
        }

        const partition = resp.topics[0].partitions[0];
        return .{
            .vote_granted = partition.vote_granted,
            .epoch = partition.leader_epoch,
        };
    }

    pub const VoteResult = struct {
        vote_granted: bool,
        epoch: i32,
    };

    fn freeVoteResponse(self: *RaftClient, resp: *generated.vote_response.VoteResponse) void {
        for (resp.topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (resp.topics.len > 0) self.allocator.free(resp.topics);
    }

    /// Send AppendEntries (log replication / heartbeat) to a follower.
    /// Sends a simplified AppendEntries-style message using BeginQuorumEpoch (API 53).
    /// For actual log data, entries are serialized after the header.
    pub fn sendAppendEntries(
        self: *RaftClient,
        leader_epoch: i32,
        leader_id: i32,
        prev_log_offset: i64,
        prev_log_epoch: i32,
        leader_commit: u64,
        entries_start_index: u64,
        entries: ?[]const []const u8,
    ) !bool {
        self.ensureConnected() catch |err| {
            log.warn("Cannot connect to peer {d}: {}", .{ self.peer_id, err });
            return err;
        };

        const corr_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        const client_id = "raft-client";
        var total_len: usize = 4 + 2 + 2 + 4 + ser.stringSize(client_id) + 2 + 4 + 4 + 4 + 8 + 4 + 8 + 8 + 4;
        if (entries) |ents| {
            for (ents) |entry_data| {
                if (entry_data.len > std.math.maxInt(i32)) return error.MessageTooLarge;
                total_len += 4 + entry_data.len;
            }
        }

        var req_buf = try self.allocator.alloc(u8, total_len);
        defer self.allocator.free(req_buf);
        var pos: usize = 4; // skip frame size

        // Use API key 53 (BeginQuorumEpoch) for heartbeats / append entries
        // Request header v1 (non-flexible)
        ser.writeI16(req_buf, &pos, 53); // api_key
        ser.writeI16(req_buf, &pos, 0); // version
        ser.writeI32(req_buf, &pos, corr_id);
        ser.writeString(req_buf, &pos, client_id);

        // BeginQuorumEpoch v0 body with empty cluster_id/topics, followed by
        // ZMQ's internal AppendEntries payload.
        ser.writeString(req_buf, &pos, ""); // cluster_id
        ser.writeI32(req_buf, &pos, 0); // empty topics array

        ser.writeI32(req_buf, &pos, leader_id);
        ser.writeI32(req_buf, &pos, leader_epoch);
        // Extra fields for append entries
        ser.writeI64(req_buf, &pos, prev_log_offset);
        ser.writeI32(req_buf, &pos, prev_log_epoch);
        ser.writeI64(req_buf, &pos, @intCast(leader_commit));
        ser.writeI64(req_buf, &pos, @intCast(entries_start_index));

        // Write entry count and data
        if (entries) |ents| {
            ser.writeI32(req_buf, &pos, @intCast(ents.len));
            for (ents) |entry_data| {
                ser.writeI32(req_buf, &pos, @intCast(entry_data.len));
                @memcpy(req_buf[pos .. pos + entry_data.len], entry_data);
                pos += entry_data.len;
            }
        } else {
            ser.writeI32(req_buf, &pos, 0);
        }
        std.debug.assert(pos == req_buf.len);

        // Write frame size
        const frame_size: i32 = @intCast(pos - 4);
        var size_pos: usize = 0;
        ser.writeI32(req_buf, &size_pos, frame_size);

        _ = self.sslSend(req_buf) catch |err| {
            self.disconnect();
            return err;
        };

        var resp_header: [4]u8 = undefined;
        const hn = self.sslRecv(&resp_header) catch |err| {
            self.disconnect();
            return err;
        };
        if (hn < 4) {
            self.disconnect();
            return error.ShortRead;
        }

        var hpos: usize = 0;
        const resp_size: usize = @intCast(@max(ser.readI32(&resp_header, &hpos), 0));
        if (resp_size < 6 or resp_size > 1024 * 1024) {
            self.disconnect();
            return error.InvalidFrameSize;
        }

        var resp_buf = try self.allocator.alloc(u8, resp_size);
        defer self.allocator.free(resp_buf);
        var total_read: usize = 0;
        while (total_read < resp_size) {
            const n = self.sslRecv(resp_buf[total_read..]) catch |err| {
                self.disconnect();
                return err;
            };
            if (n == 0) {
                self.disconnect();
                return error.ConnectionClosed;
            }
            total_read += n;
        }

        var rpos: usize = 0;
        _ = ser.readI32(resp_buf, &rpos); // correlation_id
        const error_code = ser.readI16(resp_buf, &rpos);
        return error_code == 0;
    }

    /// Send a BeginQuorumEpoch request (API key 53) as a heartbeat.
    pub fn sendBeginQuorumEpoch(
        self: *RaftClient,
        leader_epoch: i32,
        leader_id: i32,
    ) !void {
        const Req = generated.begin_quorum_epoch_request.BeginQuorumEpochRequest;

        const corr_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        var req_buf: [512]u8 = undefined;
        var pos: usize = 0;

        ser.writeI16(&req_buf, &pos, 53); // api_key
        ser.writeI16(&req_buf, &pos, 0); // version
        ser.writeI32(&req_buf, &pos, corr_id);
        ser.writeString(&req_buf, &pos, "raft-client"); // client_id

        const partitions = [_]Req.TopicData.PartitionData{.{
            .partition_index = 0,
            .leader_id = leader_id,
            .leader_epoch = leader_epoch,
        }};
        const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
        const req = Req{
            .cluster_id = "",
            .topics = &topics,
        };
        req.serialize(&req_buf, &pos, 0);

        const response = try self.sendRawRequest(req_buf[0..pos], self.allocator);
        self.allocator.free(response);
    }

    /// Send a raw framed request and return the raw response bytes (caller-owned).
    /// Used by MetadataClient for BrokerRegistration, BrokerHeartbeat, DescribeQuorum.
    pub fn sendRawRequest(self: *RaftClient, request: []const u8, alloc: Allocator) ![]u8 {
        self.ensureConnected() catch |err| {
            return err;
        };

        // Build frame: [4-byte size] [request payload]
        var frame_buf = try alloc.alloc(u8, 4 + request.len);
        defer alloc.free(frame_buf);
        var size_pos: usize = 0;
        ser.writeI32(frame_buf, &size_pos, @intCast(request.len));
        @memcpy(frame_buf[4..], request);

        _ = self.sslSend(frame_buf) catch |err| {
            self.disconnect();
            return err;
        };

        // Read response frame: 4-byte size + payload
        var resp_header: [4]u8 = undefined;
        const hn = self.sslRecv(&resp_header) catch |err| {
            self.disconnect();
            return err;
        };
        if (hn < 4) {
            self.disconnect();
            return error.ShortRead;
        }

        var hpos: usize = 0;
        const resp_size: usize = @intCast(@max(ser.readI32(&resp_header, &hpos), 0));
        if (resp_size == 0 or resp_size > 1024 * 1024) {
            self.disconnect();
            return error.InvalidFrameSize;
        }

        const resp_buf = try alloc.alloc(u8, resp_size);
        errdefer alloc.free(resp_buf);
        var total_read: usize = 0;
        while (total_read < resp_size) {
            const n = self.sslRecv(resp_buf[total_read..]) catch |err| {
                self.disconnect();
                return err;
            };
            if (n == 0) {
                self.disconnect();
                return error.ConnectionClosed;
            }
            total_read += n;
        }

        return resp_buf;
    }
};

/// Pool of RaftClient connections to all peer brokers.
pub const RaftClientPool = struct {
    clients: std.AutoHashMap(i32, RaftClient),
    allocator: Allocator,
    /// Shared client TLS context, propagated to each RaftClient on addPeer
    tls_client_ctx: ?*TlsClientContext = null,

    pub fn init(alloc: Allocator) RaftClientPool {
        return .{
            .clients = std.AutoHashMap(i32, RaftClient).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *RaftClientPool) void {
        var it = self.clients.iterator();
        while (it.next()) |entry| {
            // Free the duped host string allocated in addPeer
            self.allocator.free(entry.value_ptr.host);
            entry.value_ptr.deinit();
        }
        self.clients.deinit();
    }

    /// Set the TLS context for all future (and existing) peer connections.
    pub fn setTlsContext(self: *RaftClientPool, ctx: *TlsClientContext) void {
        self.tls_client_ctx = ctx;
        // Update existing clients
        var it = self.clients.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.tls_ctx = ctx;
        }
    }

    /// Add a peer broker to the pool. Dupes the host string for safety.
    pub fn addPeer(self: *RaftClientPool, peer_id: i32, host: []const u8, port: u16) !void {
        try self.addOrUpdatePeer(peer_id, host, port);
    }

    /// Add or replace a peer address. Existing connections are closed before
    /// replacement so dynamic voter endpoint updates do not leak stale clients.
    pub fn addOrUpdatePeer(self: *RaftClientPool, peer_id: i32, host: []const u8, port: u16) !void {
        if (self.clients.getPtr(peer_id)) |existing| {
            if (existing.port == port and std.mem.eql(u8, existing.host, host)) {
                existing.tls_ctx = self.tls_client_ctx;
                return;
            }
        }

        self.removePeer(peer_id);

        const host_owned = try self.allocator.dupe(u8, host);
        var client = RaftClient.init(self.allocator, peer_id, host_owned, port);
        client.tls_ctx = self.tls_client_ctx;
        self.clients.put(peer_id, client) catch |err| {
            self.allocator.free(host_owned);
            return err;
        };
    }

    /// Remove a peer and close any active connection.
    pub fn removePeer(self: *RaftClientPool, peer_id: i32) void {
        if (self.clients.fetchRemove(peer_id)) |removed| {
            var client = removed.value;
            client.deinit();
            self.allocator.free(client.host);
        }
    }

    /// Get the client for a specific peer.
    pub fn getClient(self: *RaftClientPool, peer_id: i32) ?*RaftClient {
        return self.clients.getPtr(peer_id);
    }

    /// Send vote requests to all peers. Returns number of votes granted.
    pub fn broadcastVoteRequest(
        self: *RaftClientPool,
        cluster_id: []const u8,
        candidate_epoch: i32,
        candidate_id: i32,
        last_log_offset: i64,
        last_log_epoch: i32,
    ) u32 {
        var votes_granted: u32 = 1; // We vote for ourselves
        var it = self.clients.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == candidate_id) continue; // Skip self
            const result = entry.value_ptr.sendVoteRequest(
                cluster_id,
                candidate_epoch,
                candidate_id,
                last_log_offset,
                last_log_epoch,
            ) catch |err| {
                log.warn("Vote RPC to peer {d} failed: {}", .{ entry.key_ptr.*, err });
                continue;
            };
            log.info("Vote from peer {d}: granted={}", .{ entry.key_ptr.*, result.vote_granted });
            if (result.vote_granted) votes_granted += 1;
        }
        return votes_granted;
    }

    /// Send heartbeats to all peers (leader only).
    pub fn broadcastHeartbeat(self: *RaftClientPool, leader_epoch: i32, leader_id: i32) void {
        var it = self.clients.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == leader_id) continue; // Skip self
            entry.value_ptr.sendBeginQuorumEpoch(leader_epoch, leader_id) catch {};
        }
    }

    /// Send AppendEntries to a specific follower.
    pub fn sendAppendEntriesToFollower(
        self: *RaftClientPool,
        follower_id: i32,
        leader_epoch: i32,
        leader_id: i32,
        prev_log_offset: i64,
        prev_log_epoch: i32,
        leader_commit: u64,
        entries_start_index: u64,
        entries: ?[]const []const u8,
    ) bool {
        const client = self.clients.getPtr(follower_id) orelse return false;
        return client.sendAppendEntries(
            leader_epoch,
            leader_id,
            prev_log_offset,
            prev_log_epoch,
            leader_commit,
            entries_start_index,
            entries,
        ) catch false;
    }

    /// Send a raw request to a specific peer and return the response (caller-owned).
    /// Used by MetadataClient for BrokerRegistration and BrokerHeartbeat.
    pub fn sendRequest(self: *RaftClientPool, peer_id: i32, request: []const u8) ?[]u8 {
        const client = self.clients.getPtr(peer_id) orelse return null;
        return client.sendRawRequest(request, self.allocator) catch |err| {
            log.warn("Raw RPC to peer {d} failed: {}", .{ peer_id, err });
            return null;
        };
    }

    /// Send DescribeQuorum (API 55) to a specific peer and return the raw response.
    /// Used by MetadataClient to discover the controller leader.
    pub fn sendDescribeQuorum(self: *RaftClientPool, peer_id: i32) ?[]u8 {
        const client = self.clients.getPtr(peer_id) orelse return null;
        const Req = generated.describe_quorum_request.DescribeQuorumRequest;

        // Build DescribeQuorum request (API 55, version 0)
        var req_buf: [128]u8 = undefined;
        var pos: usize = 0;

        // Request header v2 because DescribeQuorum v0 is flexible.
        ser.writeI16(&req_buf, &pos, 55); // api_key
        ser.writeI16(&req_buf, &pos, 0); // api_version
        ser.writeI32(&req_buf, &pos, client.next_correlation_id); // correlation_id
        client.next_correlation_id += 1;
        ser.writeCompactString(&req_buf, &pos, "zmq-metadata-client"); // client_id
        ser.writeEmptyTaggedFields(&req_buf, &pos);

        // DescribeQuorum body: topics array with __cluster_metadata, partition 0
        const partitions = [_]Req.TopicData.PartitionData{.{ .partition_index = 0 }};
        const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
        const req = Req{ .topics = &topics };
        req.serialize(&req_buf, &pos, 0);

        return client.sendRawRequest(req_buf[0..pos], self.allocator) catch |err| {
            log.warn("DescribeQuorum to peer {d} failed: {}", .{ peer_id, err });
            return null;
        };
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "RaftClientPool init/deinit" {
    var pool = RaftClientPool.init(std.testing.allocator);
    defer pool.deinit();

    try pool.addPeer(1, "localhost", 9093);
    try pool.addPeer(2, "localhost", 9094);

    const c1 = pool.getClient(1);
    try std.testing.expect(c1 != null);
    try std.testing.expectEqual(@as(i32, 1), c1.?.peer_id);

    const c3 = pool.getClient(3);
    try std.testing.expect(c3 == null);
}

test "RaftClientPool addOrUpdatePeer replaces and removes clients" {
    var pool = RaftClientPool.init(std.testing.allocator);
    defer pool.deinit();

    try pool.addPeer(1, "old-host", 9093);
    try std.testing.expectEqual(@as(usize, 1), pool.clients.count());

    try pool.addOrUpdatePeer(1, "new-host", 19093);
    try std.testing.expectEqual(@as(usize, 1), pool.clients.count());
    const updated = pool.getClient(1).?;
    try std.testing.expectEqualStrings("new-host", updated.host);
    try std.testing.expectEqual(@as(u16, 19093), updated.port);

    pool.removePeer(1);
    try std.testing.expectEqual(@as(usize, 0), pool.clients.count());
    try std.testing.expect(pool.getClient(1) == null);
}

test "RaftClient init" {
    var client = RaftClient.init(std.testing.allocator, 1, "localhost", 9093);
    defer client.deinit();

    try std.testing.expectEqual(@as(i32, 1), client.peer_id);
    try std.testing.expect(client.fd == null);
    try std.testing.expect(client.ssl_ptr == null);
    try std.testing.expect(client.tls_ctx == null);
}

test "RaftClientPool setTlsContext propagates to existing clients" {
    var pool = RaftClientPool.init(std.testing.allocator);
    defer pool.deinit();

    try pool.addPeer(1, "localhost", 9093);

    // Before setting TLS context, clients should have null tls_ctx
    const c1 = pool.getClient(1);
    try std.testing.expect(c1 != null);
    try std.testing.expect(c1.?.tls_ctx == null);

    // Create an uninitialized TlsClientContext and set it on the pool
    var tls_ctx = TlsClientContext{};
    defer tls_ctx.deinit();
    pool.setTlsContext(&tls_ctx);

    // Existing client should now have the TLS context
    const c1_after = pool.getClient(1);
    try std.testing.expect(c1_after != null);
    try std.testing.expect(c1_after.?.tls_ctx != null);

    // New clients should also inherit it
    try pool.addPeer(2, "localhost", 9094);
    const c2 = pool.getClient(2);
    try std.testing.expect(c2 != null);
    try std.testing.expect(c2.?.tls_ctx != null);
}

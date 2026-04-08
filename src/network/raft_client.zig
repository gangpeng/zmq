const std = @import("std");
const posix = std.posix;
const log = std.log.scoped(.raft_client);
const Allocator = std.mem.Allocator;
const ser = @import("../protocol/serialization.zig");
const header_mod = @import("../protocol/header.zig");

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
        if (self.fd) |fd| {
            posix.close(fd);
            self.fd = null;
        }
    }

    /// Ensure we have an active TCP connection to the peer.
    /// Supports both numeric IPs ("127.0.0.1") and hostnames ("node0") via DNS resolution.
    fn ensureConnected(self: *RaftClient) !void {
        if (self.fd != null) return;

        // Try numeric IP first, fall back to DNS resolution for hostnames (e.g. "node0" in Docker)
        const addr = std.net.Address.parseIp4(self.host, self.port) catch blk: {
            const host_z = std.fmt.allocPrintZ(self.allocator, "{s}", .{self.host}) catch return error.OutOfMemory;
            defer self.allocator.free(host_z);
            const addr_list = std.net.getAddressList(self.allocator, host_z, self.port) catch |err| {
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
        const fd = try posix.socket(addr.any.family, posix.SOCK.STREAM, 0);

        posix.connect(fd, &addr.any, addr.getOsSockLen()) catch |err| {
            posix.close(fd);
            return err;
        };

        self.fd = fd;
        log.info("Connected to peer {d} at {s}:{d}", .{ self.peer_id, self.host, self.port });
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
        self.ensureConnected() catch |err| {
            log.warn("Cannot connect to peer {d}: {}", .{ self.peer_id, err });
            return err;
        };

        const corr_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        // Build Vote request (API key 52, version 0)
        // Request header v2 (flexible) + body
        const api_key: i16 = 52;
        const api_version: i16 = 0;

        // Serialize request
        var req_buf: [512]u8 = undefined;
        var pos: usize = 0;

        // Reserve 4 bytes for frame size
        pos = 4;

        // Request header (v2 = flexible)
        ser.writeI16(&req_buf, &pos, api_key);
        ser.writeI16(&req_buf, &pos, api_version);
        ser.writeI32(&req_buf, &pos, corr_id);
        ser.writeCompactString(&req_buf, &pos, "raft-client"); // client_id
        ser.writeEmptyTaggedFields(&req_buf, &pos); // header tagged fields

        // Vote request body:
        // cluster_id (compact string), topics array (compact, empty), tagged fields
        ser.writeCompactString(&req_buf, &pos, cluster_id);
        // Voter ID
        ser.writeI32(&req_buf, &pos, candidate_id);
        // Candidate epoch
        ser.writeI32(&req_buf, &pos, candidate_epoch);
        // Candidate ID
        ser.writeI32(&req_buf, &pos, candidate_id);
        // Last epoch end offset
        ser.writeI64(&req_buf, &pos, last_log_offset);
        // Last epoch
        ser.writeI32(&req_buf, &pos, last_log_epoch);
        ser.writeEmptyTaggedFields(&req_buf, &pos); // body tagged fields

        // Write frame size at beginning
        const frame_size: i32 = @intCast(pos - 4);
        var size_pos: usize = 0;
        ser.writeI32(&req_buf, &size_pos, frame_size);

        // Send
        const fd = self.fd.?;
        _ = posix.send(fd, req_buf[0..pos], 0) catch |err| {
            self.disconnect();
            return err;
        };

        // Read response
        var resp_buf: [256]u8 = undefined;
        const n = posix.recv(fd, &resp_buf, 0) catch |err| {
            self.disconnect();
            return err;
        };

        if (n < 8) {
            self.disconnect();
            return error.ShortRead;
        }

        // Parse response: skip 4-byte frame size
        var rpos: usize = 4;
        // Response header v1 (flexible): correlation_id(4) + tagged_fields
        const resp_corr = ser.readI32(&resp_buf, &rpos);
        _ = resp_corr;
        ser.skipTaggedFields(&resp_buf, &rpos) catch {};

        // Vote response body: error_code(i16) + leader_epoch(i32) + vote_granted(bool) + tagged_fields
        if (rpos + 2 > n) {
            self.disconnect();
            return error.ShortRead;
        }
        const error_code = ser.readI16(&resp_buf, &rpos);

        // Parse leader_epoch and vote_granted if available
        var granted = (error_code == 0);
        if (rpos + 4 + 1 <= n) {
            const leader_epoch = ser.readI32(&resp_buf, &rpos);
            _ = leader_epoch;
            granted = ser.readBool(&resp_buf, &rpos) catch granted;
        }

        return .{ .vote_granted = granted, .epoch = candidate_epoch };
    }

    pub const VoteResult = struct {
        vote_granted: bool,
        epoch: i32,
    };

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
        entries: ?[]const []const u8,
    ) !bool {
        self.ensureConnected() catch |err| {
            log.warn("Cannot connect to peer {d}: {}", .{ self.peer_id, err });
            return err;
        };

        const corr_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        var req_buf: [8192]u8 = undefined;
        var pos: usize = 4; // skip frame size

        // Use API key 53 (BeginQuorumEpoch) for heartbeats / append entries
        // Request header v1 (non-flexible)
        ser.writeI16(&req_buf, &pos, 53); // api_key
        ser.writeI16(&req_buf, &pos, 0); // version
        ser.writeI32(&req_buf, &pos, corr_id);
        ser.writeString(&req_buf, &pos, "raft-client");

        // Body: error_code + topics_count(0) + leader_id + leader_epoch
        // We repurpose the body to carry append entries metadata
        ser.writeI16(&req_buf, &pos, 0); // error_code
        ser.writeI32(&req_buf, &pos, 0); // empty topics array

        ser.writeI32(&req_buf, &pos, leader_id);
        ser.writeI32(&req_buf, &pos, leader_epoch);
        // Extra fields for append entries
        ser.writeI64(&req_buf, &pos, prev_log_offset);
        ser.writeI32(&req_buf, &pos, prev_log_epoch);
        ser.writeI64(&req_buf, &pos, @intCast(leader_commit));

        // Write entry count and data
        if (entries) |ents| {
            ser.writeI32(&req_buf, &pos, @intCast(ents.len));
            for (ents) |entry_data| {
                ser.writeI32(&req_buf, &pos, @intCast(entry_data.len));
                if (pos + entry_data.len <= req_buf.len) {
                    @memcpy(req_buf[pos .. pos + entry_data.len], entry_data);
                    pos += entry_data.len;
                }
            }
        } else {
            ser.writeI32(&req_buf, &pos, 0);
        }

        // Write frame size
        const frame_size: i32 = @intCast(pos - 4);
        var size_pos: usize = 0;
        ser.writeI32(&req_buf, &size_pos, frame_size);

        const fd = self.fd.?;
        _ = posix.send(fd, req_buf[0..pos], 0) catch |err| {
            self.disconnect();
            return err;
        };

        // Read and parse response
        var resp_buf: [256]u8 = undefined;
        const n = posix.recv(fd, &resp_buf, 0) catch |err| {
            self.disconnect();
            return err;
        };

        if (n < 8) {
            self.disconnect();
            return error.ShortRead;
        }

        return true; // Heartbeat/append acknowledged
    }

    /// Send a BeginQuorumEpoch request (API key 53) as a heartbeat.
    pub fn sendBeginQuorumEpoch(
        self: *RaftClient,
        leader_epoch: i32,
        leader_id: i32,
    ) !void {
        self.ensureConnected() catch |err| {
            log.warn("Cannot connect to peer {d}: {}", .{ self.peer_id, err });
            return err;
        };

        const corr_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        var req_buf: [512]u8 = undefined;
        var pos: usize = 4; // skip frame size

        // Request header v1 (non-flexible for this API)
        ser.writeI16(&req_buf, &pos, 53); // api_key
        ser.writeI16(&req_buf, &pos, 0); // version
        ser.writeI32(&req_buf, &pos, corr_id);
        ser.writeString(&req_buf, &pos, "raft-client"); // client_id

        // BeginQuorumEpoch body:
        // Error code
        ser.writeI16(&req_buf, &pos, 0);
        // Topics array (int32 len = 0)
        ser.writeI32(&req_buf, &pos, 0);
        // Leader ID
        ser.writeI32(&req_buf, &pos, leader_id);
        // Leader epoch
        ser.writeI32(&req_buf, &pos, leader_epoch);

        // Write frame size
        const frame_size: i32 = @intCast(pos - 4);
        var size_pos: usize = 0;
        ser.writeI32(&req_buf, &size_pos, frame_size);

        const fd = self.fd.?;
        _ = posix.send(fd, req_buf[0..pos], 0) catch |err| {
            self.disconnect();
            return err;
        };

        // Read and discard response
        var resp_buf: [256]u8 = undefined;
        _ = posix.recv(fd, &resp_buf, 0) catch |err| {
            self.disconnect();
            return err;
        };
    }

    /// Send a raw framed request and return the raw response bytes (caller-owned).
    /// Used by MetadataClient for BrokerRegistration, BrokerHeartbeat, DescribeQuorum.
    pub fn sendRawRequest(self: *RaftClient, request: []const u8, alloc: Allocator) ![]u8 {
        self.ensureConnected() catch |err| {
            return err;
        };

        const fd = self.fd.?;

        // Build frame: [4-byte size] [request payload]
        var frame_buf = try alloc.alloc(u8, 4 + request.len);
        defer alloc.free(frame_buf);
        var size_pos: usize = 0;
        ser.writeI32(frame_buf, &size_pos, @intCast(request.len));
        @memcpy(frame_buf[4..], request);

        _ = posix.send(fd, frame_buf, 0) catch |err| {
            self.disconnect();
            return err;
        };

        // Read response frame: 4-byte size + payload
        var resp_header: [4]u8 = undefined;
        const hn = posix.recv(fd, &resp_header, 0) catch |err| {
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
            const n = posix.recv(fd, resp_buf[total_read..], 0) catch |err| {
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

    pub fn init(alloc: Allocator) RaftClientPool {
        return .{
            .clients = std.AutoHashMap(i32, RaftClient).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *RaftClientPool) void {
        var it = self.clients.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.clients.deinit();
    }

    /// Add a peer broker to the pool. Dupes the host string for safety.
    pub fn addPeer(self: *RaftClientPool, peer_id: i32, host: []const u8, port: u16) !void {
        const host_owned = try self.allocator.dupe(u8, host);
        try self.clients.put(peer_id, RaftClient.init(self.allocator, peer_id, host_owned, port));
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
        entries: ?[]const []const u8,
    ) bool {
        const client = self.clients.getPtr(follower_id) orelse return false;
        return client.sendAppendEntries(
            leader_epoch,
            leader_id,
            prev_log_offset,
            prev_log_epoch,
            leader_commit,
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

        // Build DescribeQuorum request (API 55, version 0)
        var req_buf: [128]u8 = undefined;
        var pos: usize = 0;

        // Request header v1
        ser.writeI16(&req_buf, &pos, 55); // api_key
        ser.writeI16(&req_buf, &pos, 0); // api_version
        ser.writeI32(&req_buf, &pos, client.next_correlation_id); // correlation_id
        client.next_correlation_id += 1;
        ser.writeString(&req_buf, &pos, "zmq-metadata-client"); // client_id

        // DescribeQuorum body: topics array with __cluster_metadata, partition 0
        ser.writeI32(&req_buf, &pos, 1); // num_topics = 1
        ser.writeString(&req_buf, &pos, "__cluster_metadata");
        ser.writeI32(&req_buf, &pos, 1); // num_partitions = 1
        ser.writeI32(&req_buf, &pos, 0); // partition_index = 0

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

test "RaftClient init" {
    var client = RaftClient.init(std.testing.allocator, 1, "localhost", 9093);
    defer client.deinit();

    try std.testing.expectEqual(@as(i32, 1), client.peer_id);
    try std.testing.expect(client.fd == null);
}

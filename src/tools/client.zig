const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const net = std.net;
const ser = @import("../protocol/serialization.zig");

/// Kafka CLI client — connects to broker via wire protocol.
///
/// Used by CLI tools (topics, console-producer, console-consumer).
pub const KafkaClient = struct {
    address: net.Address,
    sock: ?std.posix.socket_t = null,
    allocator: Allocator,
    next_correlation_id: i32 = 1,

    pub fn init(alloc: Allocator, host: []const u8, port: u16) !KafkaClient {
        return .{
            .address = try net.Address.parseIp4(host, port),
            .allocator = alloc,
        };
    }

    pub fn connect(self: *KafkaClient) !void {
        self.sock = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        try std.posix.connect(self.sock.?, &self.address.any, self.address.getOsSockLen());
    }

    pub fn close(self: *KafkaClient) void {
        if (self.sock) |s| {
            std.posix.close(s);
            self.sock = null;
        }
    }

    /// Send a raw Kafka request and receive the response.
    pub fn sendRequest(self: *KafkaClient, api_key: i16, api_version: i16, body: []const u8) ![]u8 {
        const sock = self.sock orelse return error.NotConnected;
        const corr_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        // Build request: header + body
        var header_buf: [32]u8 = undefined;
        var hpos: usize = 0;
        ser.writeI16(&header_buf, &hpos, api_key);
        ser.writeI16(&header_buf, &hpos, api_version);
        ser.writeI32(&header_buf, &hpos, corr_id);
        ser.writeString(&header_buf, &hpos, "zmq-cli");

        const frame_size: u32 = @intCast(hpos + body.len);
        var size_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &size_buf, frame_size, .big);

        _ = try std.posix.write(sock, &size_buf);
        _ = try std.posix.write(sock, header_buf[0..hpos]);
        if (body.len > 0) _ = try std.posix.write(sock, body);

        // Read response
        var resp_size_buf: [4]u8 = undefined;
        const n = try std.posix.read(sock, &resp_size_buf);
        if (n < 4) return error.ShortRead;

        const resp_size = std.mem.readInt(u32, &resp_size_buf, .big);
        const resp_data = try self.allocator.alloc(u8, resp_size);
        var total_read: usize = 0;
        while (total_read < resp_size) {
            const bytes = try std.posix.read(sock, resp_data[total_read..]);
            if (bytes == 0) break;
            total_read += bytes;
        }

        return resp_data[0..total_read];
    }
};

/// Topic admin operations using the wire protocol.
pub const TopicAdmin = struct {
    client: *KafkaClient,

    pub fn init(client: *KafkaClient) TopicAdmin {
        return .{ .client = client };
    }

    /// Create a topic.
    pub fn createTopic(self: *TopicAdmin, name: []const u8, partitions: i32, replication_factor: i16) !CreateTopicResult {
        var body_buf: [512]u8 = undefined;
        var pos: usize = 0;

        ser.writeArrayLen(&body_buf, &pos, 1); // 1 topic
        ser.writeString(&body_buf, &pos, name);
        ser.writeI32(&body_buf, &pos, partitions);
        ser.writeI16(&body_buf, &pos, replication_factor);
        ser.writeArrayLen(&body_buf, &pos, 0); // 0 assignments
        ser.writeArrayLen(&body_buf, &pos, 0); // 0 configs
        ser.writeI32(&body_buf, &pos, 30000); // timeout

        const resp = try self.client.sendRequest(19, 0, body_buf[0..pos]);
        defer self.client.allocator.free(resp);

        // Parse response
        var rpos: usize = 4; // skip correlation_id
        const n_topics = ser.readI32(resp, &rpos);
        if (n_topics > 0) {
            const topic_name = (try ser.readString(resp, &rpos)) orelse "";
            const error_code = ser.readI16(resp, &rpos);
            return .{ .name = topic_name, .error_code = error_code };
        }

        return .{ .name = name, .error_code = -1 };
    }

    pub const CreateTopicResult = struct {
        name: []const u8,
        error_code: i16,
    };
};

// ---------------------------------------------------------------
// Tests (unit tests only — integration tests need a running broker)
// ---------------------------------------------------------------

test "KafkaClient init" {
    var client = try KafkaClient.init(testing.allocator, "127.0.0.1", 9092);
    defer client.close();

    try testing.expectEqual(@as(?std.posix.socket_t, null), client.sock);
}

test "TopicAdmin init" {
    var client = try KafkaClient.init(testing.allocator, "127.0.0.1", 9092);
    defer client.close();

    const admin = TopicAdmin.init(&client);
    _ = admin;
}

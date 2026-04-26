const std = @import("std");
const Allocator = std.mem.Allocator;
const Controller = @import("../controller/controller.zig").Controller;
const Broker = @import("broker").Broker;

/// Handler routing for dual-port architecture.
///
/// The controller port (default 9093) and broker port (default 9092) each
/// have their own handler function matching Server.RequestHandler signature.
/// This module provides the global pointers and wrapper functions.

/// Global controller instance (set in main.zig when controller role is active).
var global_controller: ?*Controller = null;

/// Global broker instance (set in main.zig when broker role is active).
var global_broker: ?*Broker = null;

pub fn setGlobalController(ctrl: *Controller) void {
    global_controller = ctrl;
}

pub fn setGlobalBroker(broker: *Broker) void {
    global_broker = broker;
}

/// Handler for the controller port (9093).
/// Dispatches to Controller.handleRequest — only KRaft + registration APIs.
pub fn controllerHandleRequest(request_bytes: []const u8, alloc: Allocator) ?[]u8 {
    _ = alloc;
    if (global_controller) |ctrl| {
        return ctrl.handleRequest(request_bytes);
    }
    return null;
}

/// Handler for the broker port (9092).
/// Dispatches to Broker.handleRequest — only client-facing Kafka APIs.
pub fn brokerHandleRequest(request_bytes: []const u8, alloc: Allocator) ?[]u8 {
    _ = alloc;
    if (global_broker) |broker| {
        return broker.handleRequest(request_bytes);
    }
    return null;
}

/// Group commit: flush pending S3 WAL writes after each epoll batch.
/// Called from Server.batch_flush_fn at the end of each epoll iteration.
pub fn brokerFlushPendingWal() void {
    if (global_broker) |broker| {
        _ = broker.flushPendingWal();
    }
}

/// Group commit: check if there are pending S3 WAL writes.
/// Used by the Server to reduce epoll timeout when a flush is pending.
pub fn brokerHasPendingFlush() bool {
    if (global_broker) |broker| {
        if (broker.store.s3_wal_batcher) |*batcher| {
            return batcher.hasPendingFlush();
        }
    }
    return false;
}

/// Periodic maintenance: called ~once per second from the Server's epoll loop.
/// Delegates to Broker.tick() which handles retention enforcement, compaction,
/// consumer group session eviction, transaction expiry, and metrics updates.
///
/// NOTE: AutoMQ runs these tasks on dedicated ScheduledExecutorService threads.
/// ZMQ runs them inline on the event loop because it's single-threaded.
pub fn brokerTick() void {
    if (global_broker) |broker| {
        broker.tick();
    }
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;
const ser = @import("protocol").serialization;

fn buildTestRequest(buf: []u8, api_key: i16, api_version: i16, correlation_id: i32, header_version: i16) usize {
    var pos: usize = 0;
    ser.writeI16(buf, &pos, api_key);
    ser.writeI16(buf, &pos, api_version);
    ser.writeI32(buf, &pos, correlation_id);
    if (header_version >= 2) {
        ser.writeCompactString(buf, &pos, "test-client");
        ser.writeEmptyTaggedFields(buf, &pos);
    } else if (header_version >= 1) {
        ser.writeString(buf, &pos, "test-client");
    }
    return pos;
}

test "controllerHandleRequest returns null when no controller set" {
    // Ensure clean state
    global_controller = null;
    defer {
        global_controller = null;
    }

    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 0, 42, 1);

    const response = controllerHandleRequest(buf[0..req_len], testing.allocator);
    try testing.expect(response == null);
}

test "brokerHandleRequest returns null when no broker set" {
    global_broker = null;
    defer {
        global_broker = null;
    }

    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 0, 42, 1);

    const response = brokerHandleRequest(buf[0..req_len], testing.allocator);
    try testing.expect(response == null);
}

test "setGlobalController enables request handling" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    setGlobalController(&ctrl);
    defer {
        global_controller = null;
    }

    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 0, 42, 1);

    const response = controllerHandleRequest(buf[0..req_len], testing.allocator);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Verify correlation ID
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 42), corr_id);
}

test "setGlobalBroker enables request handling" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    setGlobalBroker(&broker);
    defer {
        global_broker = null;
    }

    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 0, 42, 1);

    const response = brokerHandleRequest(buf[0..req_len], testing.allocator);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 42), corr_id);
}

test "brokerFlushPendingWal no-op when no broker" {
    global_broker = null;
    defer {
        global_broker = null;
    }

    // Should not crash
    brokerFlushPendingWal();
}

test "brokerHasPendingFlush returns false when no broker" {
    global_broker = null;
    defer {
        global_broker = null;
    }

    const result = brokerHasPendingFlush();
    try testing.expect(!result);
}

test "brokerTick no-op when no broker" {
    global_broker = null;
    defer {
        global_broker = null;
    }

    // Should not crash
    brokerTick();
}

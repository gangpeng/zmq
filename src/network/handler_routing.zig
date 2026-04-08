const std = @import("std");
const Allocator = std.mem.Allocator;
const Controller = @import("../controller/controller.zig").Controller;
const Broker = @import("../broker/handler.zig").Broker;

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

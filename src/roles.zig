const std = @import("std");
const testing = std.testing;

/// Process role configuration, matching Apache Kafka's KRaft `process.roles`.
///
/// Each ZMQ process can run as:
/// - controller: participates in Raft consensus, manages cluster metadata
/// - broker: handles Kafka client requests (produce/fetch/admin)
/// - controller,broker: combined mode (both roles in one process)
///
/// Controller-only nodes form a fixed quorum. Broker-only nodes can be
/// added/removed dynamically — they register with the controller quorum
/// and do not participate in Raft elections.
pub const ProcessRoles = struct {
    is_controller: bool,
    is_broker: bool,

    /// Combined mode: both controller and broker (default, backward-compatible).
    pub const combined = ProcessRoles{ .is_controller = true, .is_broker = true };

    /// Controller-only: Raft consensus + metadata management, no client APIs.
    pub const controller_only = ProcessRoles{ .is_controller = true, .is_broker = false };

    /// Broker-only: client APIs only, connects to controllers for metadata.
    pub const broker_only = ProcessRoles{ .is_controller = false, .is_broker = true };

    pub fn isCombined(self: ProcessRoles) bool {
        return self.is_controller and self.is_broker;
    }

    /// Parse from a CLI or config string.
    /// Accepts: "controller", "broker", "controller,broker", "broker,controller".
    pub fn parse(value: []const u8) error{InvalidProcessRoles}!ProcessRoles {
        if (std.mem.eql(u8, value, "controller")) return controller_only;
        if (std.mem.eql(u8, value, "broker")) return broker_only;
        if (std.mem.eql(u8, value, "controller,broker")) return combined;
        if (std.mem.eql(u8, value, "broker,controller")) return combined;
        return error.InvalidProcessRoles;
    }

    /// Return a human-readable string for logging.
    pub fn name(self: ProcessRoles) []const u8 {
        if (self.isCombined()) return "controller,broker";
        if (self.is_controller) return "controller";
        if (self.is_broker) return "broker";
        return "none";
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "ProcessRoles parse valid strings" {
    const ctrl = try ProcessRoles.parse("controller");
    try testing.expect(ctrl.is_controller);
    try testing.expect(!ctrl.is_broker);

    const broker = try ProcessRoles.parse("broker");
    try testing.expect(!broker.is_controller);
    try testing.expect(broker.is_broker);

    const combined1 = try ProcessRoles.parse("controller,broker");
    try testing.expect(combined1.is_controller);
    try testing.expect(combined1.is_broker);
    try testing.expect(combined1.isCombined());

    const combined2 = try ProcessRoles.parse("broker,controller");
    try testing.expect(combined2.isCombined());
}

test "ProcessRoles parse invalid string" {
    const result = ProcessRoles.parse("invalid");
    try testing.expectError(error.InvalidProcessRoles, result);
}

test "ProcessRoles name" {
    try testing.expectEqualStrings("controller,broker", ProcessRoles.combined.name());
    try testing.expectEqualStrings("controller", ProcessRoles.controller_only.name());
    try testing.expectEqualStrings("broker", ProcessRoles.broker_only.name());
}

/// ZMQ Controller
///
/// KRaft metadata controller for cluster management:
/// - Controller: Raft consensus + metadata management
/// - BrokerRegistry: tracks live broker-only nodes
/// - MetadataClient: broker→controller communication

pub const controller = @import("controller/controller.zig");
pub const broker_registry = @import("controller/broker_registry.zig");
pub const metadata_client = @import("controller/metadata_client.zig");

pub const Controller = controller.Controller;
pub const BrokerRegistry = broker_registry.BrokerRegistry;
pub const MetadataClient = metadata_client.MetadataClient;

test {
    @import("std").testing.refAllDecls(@This());
}

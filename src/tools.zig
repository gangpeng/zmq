/// ZMQ CLI Tools
///
/// Command-line utilities for interacting with the broker:
/// - KafkaClient: low-level wire protocol client
/// - TopicAdmin: topic CRUD operations
/// - CLI: high-level commands (topics, produce, consume, groups, configs, acls)

pub const client = @import("tools/client.zig");
pub const cli = @import("tools/cli.zig");

pub const KafkaClient = client.KafkaClient;
pub const TopicAdmin = client.TopicAdmin;

test {
    @import("std").testing.refAllDecls(@This());
}

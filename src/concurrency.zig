/// ZMQ Concurrency Primitives
///
/// Zig equivalents of Java's concurrent utilities:
/// - Channel(T): bounded MPMC queue (replaces BlockingQueue)
/// - Future(T): async completion primitive (replaces CompletableFuture)

pub const channel = @import("concurrency/channel.zig");

pub const Channel = channel.Channel;

test {
    @import("std").testing.refAllDecls(@This());
}

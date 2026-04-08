/// ZMQ Memory Allocators
///
/// Three-tier allocation strategy:
/// 1. Arena allocators: request-scoped lifecycle (allocate fast, free all at once)
/// 2. Pool allocators: fixed-size hot objects (network buffers, record batch headers)
/// 3. Tracking allocator: debug-mode leak detection wrapper
///
/// All allocators implement `std.mem.Allocator` for composability.

pub const pool = @import("allocators/pool.zig");
pub const tracking = @import("allocators/tracking.zig");

pub const PoolAllocator = pool.PoolAllocator;
pub const TrackingAllocator = tracking.TrackingAllocator;

test {
    @import("std").testing.refAllDecls(@This());
}

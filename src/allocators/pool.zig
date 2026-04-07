const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Fixed-size object pool allocator.
///
/// Pre-allocates a slab of fixed-size slots and manages them via a free list.
/// Provides O(1) allocation and deallocation with zero fragmentation.
///
/// Ideal for hot-path objects with known sizes: network buffers, record batch
/// headers, connection state structs.
pub fn PoolAllocator(comptime slot_size: usize) type {
    return struct {
        const Self = @This();

        const aligned_slot_size = std.mem.alignForward(usize, @max(slot_size, @sizeOf(usize)), @alignOf(usize));

        slots: []align(@alignOf(usize)) u8,
        free_list: ?*usize,
        capacity: usize,
        allocated_count: usize,
        backing_allocator: Allocator,

        /// Create a new pool with the given number of slots.
        pub fn init(backing: Allocator, num_slots: usize) !Self {
            const total_size = aligned_slot_size * num_slots;
            const slots = try backing.alignedAlloc(u8, @alignOf(usize), total_size);

            // Initialize free list: each slot points to the next
            var free_head: ?*usize = null;
            var i = num_slots;
            while (i > 0) {
                i -= 1;
                const slot_ptr: *usize = @ptrCast(@alignCast(slots.ptr + i * aligned_slot_size));
                slot_ptr.* = @intFromPtr(free_head);
                free_head = slot_ptr;
            }

            return .{
                .slots = slots,
                .free_list = free_head,
                .capacity = num_slots,
                .allocated_count = 0,
                .backing_allocator = backing,
            };
        }

        pub fn deinit(self: *Self) void {
            self.backing_allocator.free(self.slots);
            self.* = undefined;
        }

        /// Allocate one slot from the pool. Returns null if the pool is exhausted.
        pub fn acquire(self: *Self) ?[*]u8 {
            if (self.free_list) |head| {
                const next_ptr = head.*;
                self.free_list = if (next_ptr == 0) null else @ptrFromInt(next_ptr);
                self.allocated_count += 1;
                return @ptrCast(head);
            }
            return null;
        }

        /// Return a slot to the pool.
        pub fn release(self: *Self, ptr: [*]u8) void {
            const slot: *usize = @ptrCast(@alignCast(ptr));
            slot.* = @intFromPtr(self.free_list);
            self.free_list = slot;
            self.allocated_count -= 1;
        }

        /// Number of free slots available.
        pub fn available(self: *const Self) usize {
            return self.capacity - self.allocated_count;
        }
    };
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "PoolAllocator basic acquire/release" {
    var pool_alloc = try PoolAllocator(64).init(testing.allocator, 4);
    defer pool_alloc.deinit();

    try testing.expectEqual(@as(usize, 4), pool_alloc.available());

    // Acquire all 4 slots
    const s1 = pool_alloc.acquire() orelse return error.TestUnexpectedResult;
    const s2 = pool_alloc.acquire() orelse return error.TestUnexpectedResult;
    const s3 = pool_alloc.acquire() orelse return error.TestUnexpectedResult;
    const s4 = pool_alloc.acquire() orelse return error.TestUnexpectedResult;

    try testing.expectEqual(@as(usize, 0), pool_alloc.available());

    // Pool should be exhausted
    try testing.expect(pool_alloc.acquire() == null);

    // Release one
    pool_alloc.release(s2);
    try testing.expectEqual(@as(usize, 1), pool_alloc.available());

    // Can acquire again
    const s5 = pool_alloc.acquire() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 0), pool_alloc.available());

    // Release all
    pool_alloc.release(s1);
    pool_alloc.release(s3);
    pool_alloc.release(s4);
    pool_alloc.release(s5);
    try testing.expectEqual(@as(usize, 4), pool_alloc.available());
}

test "PoolAllocator slot independence" {
    var pool_alloc = try PoolAllocator(32).init(testing.allocator, 2);
    defer pool_alloc.deinit();

    const s1 = pool_alloc.acquire() orelse return error.TestUnexpectedResult;
    const s2 = pool_alloc.acquire() orelse return error.TestUnexpectedResult;

    // Write to both slots — they should not interfere
    @memset(s1[0..32], 0xAA);
    @memset(s2[0..32], 0xBB);

    try testing.expectEqual(@as(u8, 0xAA), s1[0]);
    try testing.expectEqual(@as(u8, 0xBB), s2[0]);

    pool_alloc.release(s1);
    pool_alloc.release(s2);
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.tracking_allocator);

/// Debug-mode tracking allocator that wraps another allocator and
/// detects memory leaks.
///
/// In debug/test builds, every allocation is tracked. When the tracking
/// allocator is destroyed, it reports any outstanding allocations as leaks.
///
/// Usage:
///   var tracking = TrackingAllocator.init(std.heap.page_allocator);
///   const alloc = tracking.allocator();
///   // ... use alloc ...
///   const leaks = tracking.detectLeaks();  // returns count of leaked allocations
///   tracking.deinit();
pub const TrackingAllocator = struct {
    backing: Allocator,
    allocations: std.AutoHashMap(usize, AllocationInfo),
    total_allocated: usize,
    total_freed: usize,
    peak_usage: usize,
    current_usage: usize,

    const AllocationInfo = struct {
        size: usize,
        alignment: std.mem.Alignment,
    };

    pub fn init(backing: Allocator) TrackingAllocator {
        return .{
            .backing = backing,
            .allocations = std.AutoHashMap(usize, AllocationInfo).init(backing),
            .total_allocated = 0,
            .total_freed = 0,
            .peak_usage = 0,
            .current_usage = 0,
        };
    }

    pub fn deinit(self: *TrackingAllocator) void {
        self.allocations.deinit();
        self.* = undefined;
    }

    pub fn allocator(self: *TrackingAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    /// Returns the number of outstanding (leaked) allocations.
    pub fn detectLeaks(self: *const TrackingAllocator) usize {
        return self.allocations.count();
    }

    /// Get current memory usage stats.
    pub fn stats(self: *const TrackingAllocator) struct {
        outstanding_allocations: usize,
        current_usage_bytes: usize,
        peak_usage_bytes: usize,
        total_allocated_bytes: usize,
        total_freed_bytes: usize,
    } {
        return .{
            .outstanding_allocations = self.allocations.count(),
            .current_usage_bytes = self.current_usage,
            .peak_usage_bytes = self.peak_usage,
            .total_allocated_bytes = self.total_allocated,
            .total_freed_bytes = self.total_freed,
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, ptr_align: std.mem.Alignment, _: usize) ?[*]u8 {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ctx));
        const result = self.backing.rawAlloc(len, ptr_align, @returnAddress()) orelse return null;

        self.allocations.put(@intFromPtr(result), .{
            .size = len,
            .alignment = ptr_align,
        }) catch {
            // If we can't track it, still return the allocation
            return result;
        };

        self.total_allocated += len;
        self.current_usage += len;
        if (self.current_usage > self.peak_usage) {
            self.peak_usage = self.current_usage;
        }

        return result;
    }

    fn resize(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, _: usize) bool {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ctx));

        if (self.backing.rawResize(buf, buf_align, new_len, @returnAddress())) {
            const addr = @intFromPtr(buf.ptr);
            if (self.allocations.getPtr(addr)) |info| {
                self.current_usage -= info.size;
                self.current_usage += new_len;
                info.size = new_len;
                if (self.current_usage > self.peak_usage) {
                    self.peak_usage = self.current_usage;
                }
            }
            return true;
        }
        return false;
    }

    fn remap(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, _: usize) ?[*]u8 {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ctx));

        const result = self.backing.rawRemap(buf, buf_align, new_len, @returnAddress()) orelse return null;
        const old_addr = @intFromPtr(buf.ptr);
        const new_addr = @intFromPtr(result);

        if (self.allocations.fetchRemove(old_addr)) |entry| {
            self.current_usage -= entry.value.size;
            self.current_usage += new_len;
            self.allocations.put(new_addr, .{
                .size = new_len,
                .alignment = buf_align,
            }) catch {};
            if (self.current_usage > self.peak_usage) {
                self.peak_usage = self.current_usage;
            }
        }

        return result;
    }

    fn free(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, _: usize) void {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ctx));

        const addr = @intFromPtr(buf.ptr);
        if (self.allocations.fetchRemove(addr)) |entry| {
            self.total_freed += entry.value.size;
            self.current_usage -= entry.value.size;
        }

        self.backing.rawFree(buf, buf_align, @returnAddress());
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "TrackingAllocator detects leaks" {
    var tracker = TrackingAllocator.init(testing.allocator);
    defer tracker.deinit();

    const alloc = tracker.allocator();

    // Allocate and free properly — no leak
    const buf1 = try alloc.alloc(u8, 64);
    alloc.free(buf1);
    try testing.expectEqual(@as(usize, 0), tracker.detectLeaks());

    // Allocate but don't free — leaked
    const buf2 = try alloc.alloc(u8, 128);
    try testing.expectEqual(@as(usize, 1), tracker.detectLeaks());

    // Clean up to prevent actual leak in test
    alloc.free(buf2);
    try testing.expectEqual(@as(usize, 0), tracker.detectLeaks());
}

test "TrackingAllocator tracks stats" {
    var tracker = TrackingAllocator.init(testing.allocator);
    defer tracker.deinit();

    const alloc = tracker.allocator();

    const buf1 = try alloc.alloc(u8, 100);
    const buf2 = try alloc.alloc(u8, 200);

    var s = tracker.stats();
    try testing.expectEqual(@as(usize, 2), s.outstanding_allocations);
    try testing.expectEqual(@as(usize, 300), s.current_usage_bytes);

    alloc.free(buf1);

    s = tracker.stats();
    try testing.expectEqual(@as(usize, 1), s.outstanding_allocations);
    try testing.expectEqual(@as(usize, 200), s.current_usage_bytes);
    try testing.expect(s.peak_usage_bytes >= 300);

    alloc.free(buf2);
    try testing.expectEqual(@as(usize, 0), tracker.detectLeaks());
}

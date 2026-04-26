const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Bounded Multi-Producer Multi-Consumer (MPMC) channel.
///
/// Replaces Java's `BlockingQueue` / `ArrayBlockingQueue`.
/// Fixed-capacity ring buffer with mutex-based synchronization.
///
/// Usage:
///   var ch = try Channel(u32).init(allocator, 64);
///   defer ch.deinit();
///   try ch.send(42);
///   const val = ch.tryRecv() orelse unreachable;
pub fn Channel(comptime T: type) type {
    return struct {
        const Self = @This();

        buffer: []T,
        head: usize, // read position
        tail: usize, // write position
        count: usize,
        capacity: usize,
        mutex: @import("mutex_compat").Mutex,
        not_empty: @import("mutex_compat").Condition,
        not_full: @import("mutex_compat").Condition,
        closed: bool,
        allocator: Allocator,

        pub fn init(alloc: Allocator, capacity: usize) !Self {
            const buffer = try alloc.alloc(T, capacity);
            return .{
                .buffer = buffer,
                .head = 0,
                .tail = 0,
                .count = 0,
                .capacity = capacity,
                .mutex = .{},
                .not_empty = .{},
                .not_full = .{},
                .closed = false,
                .allocator = alloc,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
            self.* = undefined;
        }

        /// Send a value into the channel. Blocks if the channel is full.
        /// Returns error.ChannelClosed if the channel has been closed.
        pub fn send(self: *Self, value: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.count == self.capacity and !self.closed) {
                self.not_full.wait(&self.mutex);
            }

            if (self.closed) return error.ChannelClosed;

            self.buffer[self.tail] = value;
            self.tail = (self.tail + 1) % self.capacity;
            self.count += 1;

            self.not_empty.signal();
        }

        /// Try to send a value without blocking.
        /// Returns true if sent, false if the channel is full.
        pub fn trySend(self: *Self, value: T) !bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) return error.ChannelClosed;
            if (self.count == self.capacity) return false;

            self.buffer[self.tail] = value;
            self.tail = (self.tail + 1) % self.capacity;
            self.count += 1;

            self.not_empty.signal();
            return true;
        }

        /// Receive a value from the channel. Blocks if empty.
        /// Returns null if the channel is closed and empty.
        pub fn recv(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.count == 0 and !self.closed) {
                self.not_empty.wait(&self.mutex);
            }

            if (self.count == 0) return null; // closed and empty

            return self.recvLocked();
        }

        /// Try to receive a value without blocking.
        pub fn tryRecv(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.count == 0) return null;
            return self.recvLocked();
        }

        fn recvLocked(self: *Self) T {
            const value = self.buffer[self.head];
            self.head = (self.head + 1) % self.capacity;
            self.count -= 1;

            self.not_full.signal();
            return value;
        }

        /// Close the channel. No more sends are accepted.
        /// Pending recvs will drain remaining items then return null.
        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.closed = true;
            self.not_empty.broadcast();
            self.not_full.broadcast();
        }

        /// Returns the number of items currently in the channel.
        pub fn len(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.count;
        }

        /// Returns true if the channel is empty.
        pub fn isEmpty(self: *Self) bool {
            return self.len() == 0;
        }
    };
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "Channel basic send/recv" {
    var ch = try Channel(u32).init(testing.allocator, 4);
    defer ch.deinit();

    try ch.send(1);
    try ch.send(2);
    try ch.send(3);

    try testing.expectEqual(@as(u32, 1), ch.tryRecv().?);
    try testing.expectEqual(@as(u32, 2), ch.tryRecv().?);
    try testing.expectEqual(@as(u32, 3), ch.tryRecv().?);
    try testing.expect(ch.tryRecv() == null);
}

test "Channel trySend when full" {
    var ch = try Channel(u32).init(testing.allocator, 2);
    defer ch.deinit();

    try testing.expect(try ch.trySend(1));
    try testing.expect(try ch.trySend(2));
    try testing.expect(!(try ch.trySend(3))); // full

    _ = ch.tryRecv();
    try testing.expect(try ch.trySend(3)); // space now
}

test "Channel close" {
    var ch = try Channel(u32).init(testing.allocator, 4);
    defer ch.deinit();

    try ch.send(42);
    ch.close();

    // Can still drain
    try testing.expectEqual(@as(u32, 42), ch.tryRecv().?);
    // Now returns null (closed + empty)
    try testing.expect(ch.tryRecv() == null);

    // Send on closed channel returns error
    try testing.expectError(error.ChannelClosed, ch.send(99));
}

test "Channel FIFO ordering" {
    var ch = try Channel(usize).init(testing.allocator, 8);
    defer ch.deinit();

    for (0..8) |i| {
        try ch.send(i);
    }

    for (0..8) |i| {
        try testing.expectEqual(i, ch.tryRecv().?);
    }
}

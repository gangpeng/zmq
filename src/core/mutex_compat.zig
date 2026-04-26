const std = @import("std");

pub const Mutex = struct {
    state: std.atomic.Value(u8) = .init(0),

    pub fn lock(self: *Mutex) void {
        while (self.state.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn unlock(self: *Mutex) void {
        self.state.store(0, .release);
    }
};

pub const Condition = struct {
    epoch: std.atomic.Value(u32) = .init(0),

    pub fn wait(self: *Condition, mutex: *Mutex) void {
        const observed = self.epoch.load(.acquire);
        mutex.unlock();
        while (self.epoch.load(.acquire) == observed) {
            @import("time_compat").sleep(1_000_000);
        }
        mutex.lock();
    }

    pub fn signal(self: *Condition) void {
        _ = self.epoch.fetchAdd(1, .release);
    }

    pub fn broadcast(self: *Condition) void {
        self.signal();
    }
};

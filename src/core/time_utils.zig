const std = @import("std");
const testing = std.testing;

/// Time utilities for Kafka timestamps.
///
/// Kafka uses 64-bit millisecond timestamps (Unix epoch) throughout
/// its protocol and storage formats.

/// Get the current wall-clock time in milliseconds since Unix epoch.
pub fn currentTimeMillis() i64 {
    const ts = @import("time_compat").milliTimestamp();
    return ts;
}

/// Get a monotonic timestamp in nanoseconds (for measuring durations).
pub fn monotonicNanos() u64 {
    return @intCast(@import("time_compat").nanoTimestamp());
}

/// Get a monotonic timestamp in milliseconds.
pub fn monotonicMillis() u64 {
    return monotonicNanos() / std.time.ns_per_ms;
}

/// Timer for measuring elapsed durations.
pub const Timer = struct {
    start_ns: u64,

    pub fn start() Timer {
        return .{ .start_ns = monotonicNanos() };
    }

    /// Elapsed time in nanoseconds.
    pub fn elapsedNanos(self: *const Timer) u64 {
        return monotonicNanos() - self.start_ns;
    }

    /// Elapsed time in microseconds.
    pub fn elapsedMicros(self: *const Timer) u64 {
        return self.elapsedNanos() / std.time.ns_per_us;
    }

    /// Elapsed time in milliseconds.
    pub fn elapsedMillis(self: *const Timer) u64 {
        return self.elapsedNanos() / std.time.ns_per_ms;
    }

    /// Reset and return elapsed nanoseconds.
    pub fn lap(self: *Timer) u64 {
        const now = monotonicNanos();
        const elapsed = now - self.start_ns;
        self.start_ns = now;
        return elapsed;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "currentTimeMillis returns reasonable value" {
    const now = currentTimeMillis();
    // Should be after 2024-01-01 and before 2040-01-01
    try testing.expect(now > 1704067200000); // 2024-01-01
    try testing.expect(now < 2208988800000); // 2040-01-01
}

test "Timer measures non-zero elapsed" {
    var timer = Timer.start();
    // Busy-wait a tiny bit
    var sum: u64 = 0;
    for (0..10000) |i| {
        sum +%= i;
    }
    std.mem.doNotOptimizeAway(sum);
    const elapsed = timer.elapsedNanos();
    try testing.expect(elapsed > 0);
}

test "Timer lap resets" {
    var timer = Timer.start();
    const lap1 = timer.lap();
    _ = lap1;
    const lap2 = timer.lap();
    _ = lap2;
    // Both laps should complete without error
}

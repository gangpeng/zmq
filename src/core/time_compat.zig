const std = @import("std");

const linux = std.os.linux;
const posix = std.posix;

pub fn nanoTimestamp() i128 {
    var ts: linux.timespec = undefined;
    switch (posix.errno(linux.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => return (@as(i128, ts.sec) * std.time.ns_per_s) + ts.nsec,
        else => return 0,
    }
}

pub fn monotonicNanoTimestamp() u64 {
    var ts: linux.timespec = undefined;
    switch (posix.errno(linux.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast((@as(i128, ts.sec) * std.time.ns_per_s) + ts.nsec),
        else => return @intCast(nanoTimestamp()),
    }
}

pub fn milliTimestamp() i64 {
    return @intCast(@divTrunc(nanoTimestamp(), std.time.ns_per_ms));
}

pub fn monotonicMilliTimestamp() u64 {
    return monotonicNanoTimestamp() / std.time.ns_per_ms;
}

pub fn timestamp() i64 {
    return @divTrunc(milliTimestamp(), 1000);
}

pub fn sleep(ns: u64) void {
    var req = linux.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    var rem: linux.timespec = undefined;
    while (posix.errno(linux.nanosleep(&req, &rem)) == .INTR) {
        req = rem;
    }
}

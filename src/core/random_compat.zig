const std = @import("std");

const linux = std.os.linux;
const posix = std.posix;

pub fn bytes(buffer: []u8) void {
    var filled: usize = 0;
    while (filled < buffer.len) {
        const rc = linux.getrandom(buffer[filled..].ptr, buffer.len - filled, 0);
        switch (posix.errno(rc)) {
            .SUCCESS => filled += @intCast(rc),
            .INTR => continue,
            else => @panic("getrandom failed"),
        }
    }
}

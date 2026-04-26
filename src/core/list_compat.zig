const std = @import("std");

pub fn writer(list: anytype) ListWriter(@TypeOf(list)) {
    return .{ .list = list };
}

fn ListWriter(comptime ListPtr: type) type {
    return struct {
        list: ListPtr,

        pub fn writeAll(self: @This(), bytes: []const u8) !void {
            try self.list.appendSlice(bytes);
        }

        pub fn writeByte(self: @This(), byte: u8) !void {
            try self.list.append(byte);
        }

        pub fn print(self: @This(), comptime fmt: []const u8, args: anytype) !void {
            const bytes = try std.fmt.allocPrint(self.list.allocator, fmt, args);
            defer self.list.allocator.free(bytes);
            try self.writeAll(bytes);
        }

        pub fn writeInt(self: @This(), comptime T: type, value: T, endian: std.builtin.Endian) !void {
            var bytes: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, &bytes, value, endian);
            try self.writeAll(&bytes);
        }
    };
}

const std = @import("std");

const Allocator = std.mem.Allocator;
const posix = std.posix;

pub const Address = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,

    pub fn parseIp4(host: []const u8, port: u16) !Address {
        var parts = std.mem.splitScalar(u8, host, '.');
        var octets: [4]u8 = undefined;
        var i: usize = 0;
        while (parts.next()) |part| {
            if (i >= 4 or part.len == 0) return error.InvalidAddress;
            octets[i] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidAddress;
            i += 1;
        }
        if (i != 4) return error.InvalidAddress;

        const addr_be = std.mem.readInt(u32, &octets, .big);
        return .{ .in = .{
            .port = std.mem.nativeToBig(u16, port),
            .addr = std.mem.nativeToBig(u32, addr_be),
        } };
    }

    pub fn getOsSockLen(_: Address) posix.socklen_t {
        return @sizeOf(posix.sockaddr.in);
    }

    pub fn getPort(self: Address) u16 {
        return std.mem.bigToNative(u16, self.in.port);
    }
};

pub const AddressList = struct {
    addrs: []Address,
    allocator: Allocator,

    pub fn deinit(self: AddressList) void {
        self.allocator.free(self.addrs);
    }
};

pub fn getAddressList(allocator: Allocator, host: []const u8, port: u16) !AddressList {
    const host_z = try allocator.dupeZ(u8, host);
    defer allocator.free(host_z);

    var service_buf: [6]u8 = undefined;
    const service_z = try std.fmt.bufPrintZ(&service_buf, "{d}", .{port});

    var hints: std.c.addrinfo = .{
        .flags = .{},
        .family = posix.AF.INET,
        .socktype = posix.SOCK.STREAM,
        .protocol = 0,
        .addrlen = 0,
        .addr = null,
        .canonname = null,
        .next = null,
    };
    var result: ?*std.c.addrinfo = null;
    if (@intFromEnum(std.c.getaddrinfo(host_z.ptr, service_z.ptr, &hints, &result)) != 0) {
        return error.UnknownHostName;
    }
    defer if (result) |res| std.c.freeaddrinfo(res);

    var count: usize = 0;
    var cur = result;
    while (cur) |ai| : (cur = ai.next) {
        if (ai.family == posix.AF.INET and ai.addr != null) count += 1;
    }

    const addrs = try allocator.alloc(Address, count);
    errdefer allocator.free(addrs);

    var idx: usize = 0;
    cur = result;
    while (cur) |ai| : (cur = ai.next) {
        if (ai.family != posix.AF.INET or ai.addr == null) continue;
        const in_addr: *const posix.sockaddr.in = @ptrCast(@alignCast(ai.addr.?));
        addrs[idx] = .{ .in = in_addr.* };
        idx += 1;
    }

    return .{ .addrs = addrs, .allocator = allocator };
}

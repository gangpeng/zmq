const std = @import("std");

const linux = std.os.linux;
const posix = std.posix;

pub fn socket(domain: u32, socket_type: u32, protocol: u32) !posix.socket_t {
    const rc = linux.socket(domain, socket_type, protocol);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .ACCES => return error.AccessDenied,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .INVAL => return error.ProtocolFamilyNotAvailable,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOBUFS => return error.SystemResources,
        .NOMEM => return error.SystemResources,
        .PROTONOSUPPORT => return error.ProtocolNotSupported,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn connect(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) !void {
    switch (posix.errno(linux.connect(fd, addr, len))) {
        .SUCCESS => return,
        .ACCES => return error.AccessDenied,
        .PERM => return error.PermissionDenied,
        .ADDRINUSE => return error.AddressInUse,
        .ADDRNOTAVAIL => return error.AddressNotAvailable,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .AGAIN => return error.WouldBlock,
        .ALREADY => return error.ConnectionPending,
        .BADF => return error.Unexpected,
        .CONNREFUSED => return error.ConnectionRefused,
        .FAULT => unreachable,
        .INPROGRESS => return error.WouldBlock,
        .INTR => return error.WouldBlock,
        .ISCONN => return error.AlreadyConnected,
        .NETUNREACH => return error.NetworkUnreachable,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        .PROTOTYPE => return error.ProtocolMismatch,
        .TIMEDOUT => return error.ConnectionTimedOut,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn bind(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) !void {
    switch (posix.errno(linux.bind(fd, addr, len))) {
        .SUCCESS => return,
        .ACCES => return error.AccessDenied,
        .ADDRINUSE => return error.AddressInUse,
        .ADDRNOTAVAIL => return error.AddressNotAvailable,
        .BADF => return error.Unexpected,
        .FAULT => unreachable,
        .INVAL => return error.Unexpected,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn listen(fd: posix.socket_t, backlog: u32) !void {
    switch (posix.errno(linux.listen(fd, backlog))) {
        .SUCCESS => return,
        .ADDRINUSE => return error.AddressInUse,
        .BADF => return error.Unexpected,
        .DESTADDRREQ => return error.SocketNotBound,
        .INVAL => return error.Unexpected,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        .OPNOTSUPP => return error.OperationNotSupported,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn accept(fd: posix.socket_t, addr: ?*posix.sockaddr, len: ?*posix.socklen_t, flags: u32) !posix.socket_t {
    const rc = if (flags == 0)
        linux.accept(fd, addr, len)
    else
        linux.accept4(fd, addr, len, flags);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .AGAIN => return error.WouldBlock,
        .BADF => return error.Unexpected,
        .CONNABORTED => return error.ConnectionAborted,
        .FAULT => unreachable,
        .INTR => return error.WouldBlock,
        .INVAL => return error.Unexpected,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOBUFS => return error.SystemResources,
        .NOMEM => return error.SystemResources,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        .OPNOTSUPP => return error.OperationNotSupported,
        .PROTO => return error.ProtocolFailure,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn send(fd: posix.fd_t, bytes: []const u8, flags: u32) !usize {
    while (true) {
        const rc = linux.sendto(fd, bytes.ptr, bytes.len, flags, null, 0);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.Unexpected,
            .CONNRESET => return error.ConnectionResetByPeer,
            .DESTADDRREQ => return error.DestinationAddressRequired,
            .FAULT => unreachable,
            .INVAL => return error.Unexpected,
            .ISCONN => return error.AlreadyConnected,
            .MSGSIZE => return error.MessageTooLarge,
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .NOTCONN => return error.SocketNotConnected,
            .NOTSOCK => return error.FileDescriptorNotASocket,
            .PIPE => return error.BrokenPipe,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn epoll_create1(flags: usize) !posix.fd_t {
    const rc = linux.epoll_create1(flags);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .INVAL => return error.Unexpected,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn epoll_ctl(epoll_fd: posix.fd_t, op: u32, fd: posix.fd_t, ev: ?*linux.epoll_event) !void {
    switch (posix.errno(linux.epoll_ctl(epoll_fd, op, fd, ev))) {
        .SUCCESS => return,
        .BADF => return error.Unexpected,
        .EXIST => return error.FileAlreadyExists,
        .INVAL => return error.Unexpected,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.SystemResources,
        .NOSPC => return error.NoSpaceLeft,
        .PERM => return error.AccessDenied,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn epoll_wait(epoll_fd: posix.fd_t, events: []linux.epoll_event, timeout: i32) usize {
    while (true) {
        const rc = linux.epoll_wait(epoll_fd, events.ptr, @intCast(events.len), timeout);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => return 0,
            else => return 0,
        }
    }
}

pub fn close(fd: posix.fd_t) void {
    while (true) {
        switch (posix.errno(linux.close(fd))) {
            .SUCCESS => return,
            .INTR => continue,
            else => return,
        }
    }
}

pub fn write(fd: posix.fd_t, bytes: []const u8) !usize {
    while (true) {
        const rc = linux.write(fd, bytes.ptr, bytes.len);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.Unexpected,
            .DQUOT => return error.DiskQuota,
            .FAULT => unreachable,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .PERM => return error.AccessDenied,
            .PIPE => return error.BrokenPipe,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn recv(fd: posix.fd_t, buffer: []u8, flags: u32) !usize {
    while (true) {
        const rc = linux.recvfrom(fd, buffer.ptr, buffer.len, flags, null, null);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.Unexpected,
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .FAULT => unreachable,
            .INVAL => return error.Unexpected,
            .NOMEM => return error.SystemResources,
            .NOTCONN => return error.SocketNotConnected,
            .NOTSOCK => return error.FileDescriptorNotASocket,
            .TIMEDOUT => return error.ConnectionTimedOut,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

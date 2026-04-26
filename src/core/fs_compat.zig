const std = @import("std");

const Allocator = std.mem.Allocator;
const linux = std.os.linux;
const posix = std.posix;

pub const OpenFileOptions = struct {
    mode: Mode = .read_only,

    pub const Mode = enum {
        read_only,
        write_only,
        read_write,
    };
};

pub const CreateFileOptions = struct {
    read: bool = false,
    truncate: bool = true,
    exclusive: bool = false,
};

pub const OpenDirOptions = struct {
    iterate: bool = false,
};

pub const Stat = struct {
    size: u64,
};

pub const File = struct {
    fd: posix.fd_t,

    pub fn close(self: File) void {
        while (true) {
            switch (posix.errno(linux.close(self.fd))) {
                .SUCCESS => return,
                .INTR => continue,
                else => return,
            }
        }
    }

    pub fn read(self: File, buffer: []u8) !usize {
        return posix.read(self.fd, buffer);
    }

    pub fn readAll(self: File, buffer: []u8) !usize {
        var total: usize = 0;
        while (total < buffer.len) {
            const n = try self.read(buffer[total..]);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    pub fn readToEndAlloc(self: File, allocator: Allocator, max_bytes: usize) ![]u8 {
        var out = std.array_list.Managed(u8).init(allocator);
        errdefer out.deinit();

        var buffer: [8192]u8 = undefined;
        while (out.items.len < max_bytes) {
            const limit = @min(buffer.len, max_bytes - out.items.len);
            const n = try self.read(buffer[0..limit]);
            if (n == 0) break;
            try out.appendSlice(buffer[0..n]);
        }

        if (out.items.len == max_bytes) {
            var one: [1]u8 = undefined;
            if (try self.read(&one) != 0) return error.FileTooBig;
        }

        return out.toOwnedSlice();
    }

    pub fn writeAll(self: File, bytes: []const u8) !void {
        var written: usize = 0;
        while (written < bytes.len) {
            const rc = linux.write(self.fd, bytes[written..].ptr, bytes.len - written);
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    const n: usize = @intCast(rc);
                    if (n == 0) return error.WriteZero;
                    written += n;
                },
                .INTR => continue,
                .AGAIN => return error.WouldBlock,
                .BADF => return error.Unexpected,
                .DESTADDRREQ => return error.Unexpected,
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

    pub fn sync(self: File) !void {
        while (true) {
            switch (posix.errno(linux.fsync(self.fd))) {
                .SUCCESS => return,
                .INTR => continue,
                .BADF => return error.Unexpected,
                .IO => return error.InputOutput,
                .NOSPC => return error.NoSpaceLeft,
                .DQUOT => return error.DiskQuota,
                else => |err| return posix.unexpectedErrno(err),
            }
        }
    }

    pub fn seekTo(self: File, offset: u64) !void {
        return seek(self.fd, @intCast(offset), linux.SEEK.SET);
    }

    pub fn seekFromEnd(self: File, offset: i64) !void {
        return seek(self.fd, offset, linux.SEEK.END);
    }

    pub fn stat(self: File) !Stat {
        var statx_buf: linux.Statx = undefined;
        const rc = linux.statx(self.fd, "", linux.AT.EMPTY_PATH, .{ .SIZE = true }, &statx_buf);
        switch (posix.errno(rc)) {
            .SUCCESS => return .{ .size = statx_buf.size },
            .BADF => return error.Unexpected,
            .FAULT => unreachable,
            .INVAL => return error.Unexpected,
            .NOMEM => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    }

    pub fn writer(self: File) Writer {
        return .{ .file = self };
    }

    fn seek(fd: posix.fd_t, offset: i64, whence: usize) !void {
        switch (posix.errno(linux.lseek(fd, offset, whence))) {
            .SUCCESS => return,
            .BADF => return error.Unexpected,
            .INVAL => return error.Unseekable,
            .OVERFLOW => return error.Unseekable,
            .SPIPE => return error.Unseekable,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
};

pub const Writer = struct {
    file: File,

    pub fn print(self: Writer, comptime fmt: []const u8, args: anytype) !void {
        const bytes = try std.fmt.allocPrint(std.heap.page_allocator, fmt, args);
        defer std.heap.page_allocator.free(bytes);
        try self.file.writeAll(bytes);
    }
};

pub const Cwd = struct {
    pub fn openFile(_: Cwd, path: []const u8, options: OpenFileOptions) !File {
        return openFileAt(posix.AT.FDCWD, path, options);
    }
};

pub fn cwd() Cwd {
    return .{};
}

pub fn openFileAbsolute(path: []const u8, options: OpenFileOptions) !File {
    return openFileAt(posix.AT.FDCWD, path, options);
}

pub fn createFileAbsolute(path: []const u8, options: CreateFileOptions) !File {
    var flags: posix.O = .{
        .ACCMODE = if (options.read) .RDWR else .WRONLY,
        .CREAT = true,
        .CLOEXEC = true,
        .TRUNC = options.truncate,
        .EXCL = options.exclusive,
    };

    // Non-truncating create is used as an append-open by the file sink.
    if (!options.truncate) flags.ACCMODE = .RDWR;

    return .{ .fd = try posix.openat(posix.AT.FDCWD, path, flags, 0o666) };
}

pub fn makeDirAbsolute(path: []const u8) !void {
    const path_z = try posix.toPosixPath(path);
    switch (posix.errno(linux.mkdir(&path_z, 0o755))) {
        .SUCCESS => return,
        .EXIST => return error.PathAlreadyExists,
        .ACCES => return error.AccessDenied,
        .PERM => return error.PermissionDenied,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .NAMETOOLONG => return error.NameTooLong,
        .NOSPC => return error.NoSpaceLeft,
        .ROFS => return error.ReadOnlyFileSystem,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn deleteFileAbsolute(path: []const u8) !void {
    const path_z = try posix.toPosixPath(path);
    switch (posix.errno(linux.unlink(&path_z))) {
        .SUCCESS => return,
        .NOENT => return error.FileNotFound,
        .ACCES => return error.AccessDenied,
        .PERM => return error.PermissionDenied,
        .ISDIR => return error.IsDir,
        .NAMETOOLONG => return error.NameTooLong,
        .NOTDIR => return error.NotDir,
        .ROFS => return error.ReadOnlyFileSystem,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn deleteTreeAbsolute(path: []const u8) !void {
    return deleteTree(path);
}

pub fn openDirAbsolute(path: []const u8, options: OpenDirOptions) !Dir {
    _ = options;
    const path_z = try posix.toPosixPath(path);
    const dir = std.c.opendir(&path_z) orelse return error.FileNotFound;
    return .{ .handle = dir };
}

pub const Dir = struct {
    handle: *std.c.DIR,

    pub fn close(self: Dir) void {
        _ = std.c.closedir(self.handle);
    }

    pub fn iterate(self: Dir) Iterator {
        return .{ .handle = self.handle };
    }
};

pub const Entry = struct {
    name: []const u8,
};

pub const Iterator = struct {
    handle: *std.c.DIR,

    pub fn next(self: *Iterator) !?Entry {
        while (true) {
            const entry = std.c.readdir(self.handle) orelse return null;
            const name = std.mem.sliceTo(entry.name[0..], 0);
            if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
            return .{ .name = name };
        }
    }
};

fn openFileAt(dir_fd: posix.fd_t, path: []const u8, options: OpenFileOptions) !File {
    const flags: posix.O = .{
        .ACCMODE = switch (options.mode) {
            .read_only => .RDONLY,
            .write_only => .WRONLY,
            .read_write => .RDWR,
        },
        .CLOEXEC = true,
    };
    return .{ .fd = try posix.openat(dir_fd, path, flags, 0) };
}

fn deleteTree(path: []const u8) !void {
    const path_z = posix.toPosixPath(path) catch return error.NameTooLong;
    const dir = std.c.opendir(&path_z) orelse {
        return deleteFileAbsolute(path) catch error.FileNotFound;
    };
    defer _ = std.c.closedir(dir);

    while (std.c.readdir(dir)) |entry| {
        const name = std.mem.sliceTo(entry.name[0..], 0);
        if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;

        const child = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/{s}", .{ path, name });
        defer std.heap.page_allocator.free(child);

        if (entry.type == std.c.DT.DIR) {
            try deleteTree(child);
        } else {
            deleteFileAbsolute(child) catch |err| switch (err) {
                error.IsDir => try deleteTree(child),
                error.FileNotFound => {},
                else => return err,
            };
        }
    }

    switch (posix.errno(linux.rmdir(&path_z))) {
        .SUCCESS => return,
        .NOENT => return error.FileNotFound,
        .NOTEMPTY => return error.Unexpected,
        .ACCES => return error.AccessDenied,
        .PERM => return error.PermissionDenied,
        .NAMETOOLONG => return error.NameTooLong,
        .NOTDIR => return error.NotDir,
        .ROFS => return error.ReadOnlyFileSystem,
        else => |err| return posix.unexpectedErrno(err),
    }
}

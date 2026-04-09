const std = @import("std");
const log = std.log.scoped(.openssl);

/// OpenSSL runtime bindings via dlopen/dlsym.
///
/// Loads libssl and libcrypto at runtime — no compile-time C headers or
/// -dev packages required. Falls back gracefully if OpenSSL is not installed.
///
/// Uses the C dlopen/dlsym interface because Zig 0.13's std.DynLib uses its
/// own ELF parser which doesn't support all hash table formats used by
/// modern OpenSSL builds.
///
/// NOTE: AutoMQ uses Java's built-in SSL/TLS (JSSE). ZMQ uses OpenSSL
/// directly because Zig has no built-in server-side TLS implementation.
pub const OpenSslLib = struct {
    ssl_handle: *anyopaque,
    crypto_handle: *anyopaque,

    // -- libssl function pointers --
    TLS_server_method: *const fn () ?*anyopaque,
    SSL_CTX_new: *const fn (?*anyopaque) ?*anyopaque,
    SSL_CTX_free: *const fn (?*anyopaque) void,
    SSL_CTX_use_certificate_chain_file: *const fn (?*anyopaque, [*:0]const u8) c_int,
    SSL_CTX_use_PrivateKey_file: *const fn (?*anyopaque, [*:0]const u8, c_int) c_int,
    SSL_CTX_check_private_key: *const fn (?*anyopaque) c_int,
    SSL_CTX_load_verify_locations: *const fn (?*anyopaque, ?[*:0]const u8, ?[*:0]const u8) c_int,
    SSL_CTX_set_verify: *const fn (?*anyopaque, c_int, ?*anyopaque) void,

    /// SSL_CTX_ctrl(ctx, cmd, larg, parg) → long — generic control function
    /// Used for set_min/max_proto_version (macros in OpenSSL headers)
    SSL_CTX_ctrl: *const fn (?*anyopaque, c_int, c_long, ?*anyopaque) c_long,
    SSL_new: *const fn (?*anyopaque) ?*anyopaque,
    SSL_free: *const fn (?*anyopaque) void,
    SSL_set_fd: *const fn (?*anyopaque, c_int) c_int,
    SSL_accept: *const fn (?*anyopaque) c_int,
    SSL_read: *const fn (?*anyopaque, [*]u8, c_int) c_int,
    SSL_write: *const fn (?*anyopaque, [*]const u8, c_int) c_int,
    SSL_shutdown: *const fn (?*anyopaque) c_int,
    SSL_get_error: *const fn (?*anyopaque, c_int) c_int,

    // -- libcrypto function pointers --
    OPENSSL_init_ssl: *const fn (u64, ?*anyopaque) c_int,
    ERR_get_error: *const fn () c_ulong,
    ERR_error_string_n: *const fn (c_ulong, [*]u8, usize) void,

    // -- OpenSSL constants --
    pub const SSL_FILETYPE_PEM: c_int = 1;
    pub const SSL_ERROR_NONE: c_int = 0;
    pub const SSL_ERROR_SSL: c_int = 1;
    pub const SSL_ERROR_WANT_READ: c_int = 2;
    pub const SSL_ERROR_WANT_WRITE: c_int = 3;
    pub const SSL_ERROR_SYSCALL: c_int = 5;
    pub const SSL_ERROR_ZERO_RETURN: c_int = 6;
    pub const SSL_VERIFY_NONE: c_int = 0;
    pub const SSL_VERIFY_PEER: c_int = 1;
    pub const SSL_VERIFY_FAIL_IF_NO_PEER_CERT: c_int = 2;
    pub const TLS1_2_VERSION: c_int = 0x0303;
    pub const TLS1_3_VERSION: c_int = 0x0304;
    pub const OPENSSL_INIT_LOAD_SSL_STRINGS: u64 = 0x00200000;
    pub const OPENSSL_INIT_LOAD_CRYPTO_STRINGS: u64 = 0x00000002;

    /// SSL_CTX_ctrl command codes (macros SSL_CTX_set_min/max_proto_version)
    pub const SSL_CTRL_SET_MIN_PROTO_VERSION: c_int = 123;
    pub const SSL_CTRL_SET_MAX_PROTO_VERSION: c_int = 124;

    /// Convenience: SSL_CTX_set_min_proto_version via SSL_CTX_ctrl
    pub fn setMinProtoVersion(self: *const OpenSslLib, ctx: ?*anyopaque, version: c_int) bool {
        return self.SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MIN_PROTO_VERSION, @as(c_long, version), null) != 0;
    }

    /// Convenience: SSL_CTX_set_max_proto_version via SSL_CTX_ctrl
    pub fn setMaxProtoVersion(self: *const OpenSslLib, ctx: ?*anyopaque, version: c_int) bool {
        return self.SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MAX_PROTO_VERSION, @as(c_long, version), null) != 0;
    }

    // -- C dlopen/dlsym/dlclose --
    const RTLD_LAZY: c_int = 1;
    extern "c" fn dlopen(path: [*:0]const u8, flags: c_int) ?*anyopaque;
    extern "c" fn dlsym(handle: *anyopaque, symbol: [*:0]const u8) ?*anyopaque;
    extern "c" fn dlclose(handle: *anyopaque) c_int;

    fn lookupFn(handle: *anyopaque, comptime T: type, name: [*:0]const u8) !T {
        const ptr = dlsym(handle, name) orelse {
            log.err("OpenSSL symbol not found: {s}", .{name});
            return error.SymbolNotFound;
        };
        return @ptrCast(ptr);
    }

    /// Load OpenSSL libraries at runtime.
    /// Tries libssl.so.3 first (OpenSSL 3.x), then libssl.so.
    pub fn load() !OpenSslLib {
        const ssl_handle = dlopen("libssl.so.3", RTLD_LAZY) orelse
            dlopen("libssl.so", RTLD_LAZY) orelse {
            log.err("Failed to load libssl — TLS not available. Install OpenSSL.", .{});
            return error.OpenSslNotAvailable;
        };
        errdefer _ = dlclose(ssl_handle);

        const crypto_handle = dlopen("libcrypto.so.3", RTLD_LAZY) orelse
            dlopen("libcrypto.so", RTLD_LAZY) orelse {
            log.err("Failed to load libcrypto — TLS not available.", .{});
            return error.OpenSslNotAvailable;
        };
        errdefer _ = dlclose(crypto_handle);

        var self = OpenSslLib{
            .ssl_handle = ssl_handle,
            .crypto_handle = crypto_handle,
            .TLS_server_method = undefined,
            .SSL_CTX_new = undefined,
            .SSL_CTX_free = undefined,
            .SSL_CTX_use_certificate_chain_file = undefined,
            .SSL_CTX_use_PrivateKey_file = undefined,
            .SSL_CTX_check_private_key = undefined,
            .SSL_CTX_load_verify_locations = undefined,
            .SSL_CTX_set_verify = undefined,
            .SSL_CTX_ctrl = undefined,
            .SSL_new = undefined,
            .SSL_free = undefined,
            .SSL_set_fd = undefined,
            .SSL_accept = undefined,
            .SSL_read = undefined,
            .SSL_write = undefined,
            .SSL_shutdown = undefined,
            .SSL_get_error = undefined,
            .OPENSSL_init_ssl = undefined,
            .ERR_get_error = undefined,
            .ERR_error_string_n = undefined,
        };

        // Load libssl functions
        self.TLS_server_method = try lookupFn(ssl_handle, @TypeOf(self.TLS_server_method), "TLS_server_method");
        self.SSL_CTX_new = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_new), "SSL_CTX_new");
        self.SSL_CTX_free = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_free), "SSL_CTX_free");
        self.SSL_CTX_use_certificate_chain_file = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_use_certificate_chain_file), "SSL_CTX_use_certificate_chain_file");
        self.SSL_CTX_use_PrivateKey_file = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_use_PrivateKey_file), "SSL_CTX_use_PrivateKey_file");
        self.SSL_CTX_check_private_key = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_check_private_key), "SSL_CTX_check_private_key");
        self.SSL_CTX_load_verify_locations = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_load_verify_locations), "SSL_CTX_load_verify_locations");
        self.SSL_CTX_set_verify = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_set_verify), "SSL_CTX_set_verify");
        self.SSL_CTX_ctrl = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_ctrl), "SSL_CTX_ctrl");
        self.SSL_new = try lookupFn(ssl_handle, @TypeOf(self.SSL_new), "SSL_new");
        self.SSL_free = try lookupFn(ssl_handle, @TypeOf(self.SSL_free), "SSL_free");
        self.SSL_set_fd = try lookupFn(ssl_handle, @TypeOf(self.SSL_set_fd), "SSL_set_fd");
        self.SSL_accept = try lookupFn(ssl_handle, @TypeOf(self.SSL_accept), "SSL_accept");
        self.SSL_read = try lookupFn(ssl_handle, @TypeOf(self.SSL_read), "SSL_read");
        self.SSL_write = try lookupFn(ssl_handle, @TypeOf(self.SSL_write), "SSL_write");
        self.SSL_shutdown = try lookupFn(ssl_handle, @TypeOf(self.SSL_shutdown), "SSL_shutdown");
        self.SSL_get_error = try lookupFn(ssl_handle, @TypeOf(self.SSL_get_error), "SSL_get_error");

        // Load libcrypto/libssl init functions
        self.OPENSSL_init_ssl = try lookupFn(ssl_handle, @TypeOf(self.OPENSSL_init_ssl), "OPENSSL_init_ssl");
        self.ERR_get_error = try lookupFn(crypto_handle, @TypeOf(self.ERR_get_error), "ERR_get_error");
        self.ERR_error_string_n = try lookupFn(crypto_handle, @TypeOf(self.ERR_error_string_n), "ERR_error_string_n");

        // Initialize OpenSSL
        _ = self.OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS | OPENSSL_INIT_LOAD_CRYPTO_STRINGS, null);

        log.info("OpenSSL loaded successfully", .{});
        return self;
    }

    /// Get a human-readable error string from the OpenSSL error queue.
    pub fn getErrorString(self: *const OpenSslLib) [256]u8 {
        var buf: [256]u8 = [_]u8{0} ** 256;
        const err = self.ERR_get_error();
        if (err != 0) {
            self.ERR_error_string_n(err, &buf, buf.len);
        }
        return buf;
    }

    /// Close the dynamic libraries.
    pub fn close(self: *OpenSslLib) void {
        _ = dlclose(self.ssl_handle);
        _ = dlclose(self.crypto_handle);
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "OpenSslLib load and create context" {
    var lib = OpenSslLib.load() catch |err| {
        log.warn("OpenSSL not available, skipping test: {}", .{err});
        return;
    };
    defer lib.close();

    // Create TLS server method
    const method = lib.TLS_server_method();
    try testing.expect(method != null);

    // Create SSL_CTX
    const ctx = lib.SSL_CTX_new(method);
    try testing.expect(ctx != null);

    // Set TLS 1.2 minimum
    try testing.expect(lib.setMinProtoVersion(ctx, OpenSslLib.TLS1_2_VERSION));

    lib.SSL_CTX_free(ctx);
}

test "OpenSslLib error string does not crash" {
    var lib = OpenSslLib.load() catch return;
    defer lib.close();
    const err_str = lib.getErrorString();
    _ = err_str;
}

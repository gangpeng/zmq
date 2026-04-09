const std = @import("std");
const testing = std.testing;
const posix = std.posix;
const log = std.log.scoped(.tls);
const Allocator = std.mem.Allocator;
const OpenSslLib = @import("openssl.zig").OpenSslLib;

/// TLS configuration for the broker.
///
/// Supports both client-facing (data plane) and inter-broker (control plane)
/// TLS connections. Uses OpenSSL via runtime dlopen — no compile-time C
/// headers required, just libssl.so.3 at runtime.
///
/// Kafka TLS modes:
/// - PLAINTEXT (port 9092): No TLS
/// - SSL (port 9093): TLS for all connections
/// - SASL_PLAINTEXT: SASL auth, no TLS
/// - SASL_SSL: Both SASL auth and TLS
pub const TlsConfig = struct {
    enabled: bool = false,
    protocol: SecurityProtocol = .plaintext,

    // Certificate paths
    keystore_path: ?[]const u8 = null,
    keystore_password: ?[]const u8 = null,
    truststore_path: ?[]const u8 = null,
    truststore_password: ?[]const u8 = null,

    // PEM-based config (alternative to JKS keystore)
    cert_file: ?[]const u8 = null,
    key_file: ?[]const u8 = null,
    ca_file: ?[]const u8 = null,

    // Client authentication
    client_auth: ClientAuth = .none,

    // TLS version constraints
    min_tls_version: TlsVersion = .tls_1_2,
    max_tls_version: TlsVersion = .tls_1_3,

    pub const SecurityProtocol = enum {
        plaintext,
        ssl,
        sasl_plaintext,
        sasl_ssl,
    };

    pub const ClientAuth = enum {
        none,
        requested,
        required,
    };

    pub const TlsVersion = enum {
        tls_1_0,
        tls_1_1,
        tls_1_2,
        tls_1_3,

        pub fn toOpenSsl(self: TlsVersion) c_int {
            return switch (self) {
                .tls_1_0 => 0x0301,
                .tls_1_1 => 0x0302,
                .tls_1_2 => OpenSslLib.TLS1_2_VERSION,
                .tls_1_3 => OpenSslLib.TLS1_3_VERSION,
            };
        }
    };

    /// Check if TLS is needed for this protocol.
    pub fn needsTls(self: *const TlsConfig) bool {
        return self.protocol == .ssl or self.protocol == .sasl_ssl;
    }

    /// Check if SASL is needed for this protocol.
    pub fn needsSasl(self: *const TlsConfig) bool {
        return self.protocol == .sasl_plaintext or self.protocol == .sasl_ssl;
    }

    /// Validate the configuration.
    pub fn validate(self: *const TlsConfig) !void {
        if (!self.needsTls()) return;

        if (self.cert_file == null and self.keystore_path == null) {
            return error.NoCertificateConfigured;
        }
        if (self.key_file == null and self.keystore_path == null) {
            return error.NoPrivateKeyConfigured;
        }
    }
};

/// TLS connection state for a single client connection.
///
/// Wraps the raw TCP socket with TLS encryption/decryption via OpenSSL.
/// Handles non-blocking TLS handshake (SSL_accept returns WANT_READ/WANT_WRITE)
/// and encrypted I/O (SSL_read/SSL_write).
pub const TlsConnection = struct {
    inner_fd: posix.fd_t,
    tls_established: bool = false,
    ssl_ptr: ?*anyopaque = null,
    openssl: ?*OpenSslLib = null,
    peer_cert_subject: ?[]const u8 = null,
    allocator: Allocator,

    pub fn init(alloc: Allocator, fd: posix.fd_t) TlsConnection {
        return .{
            .inner_fd = fd,
            .allocator = alloc,
        };
    }

    /// Initialize with an OpenSSL SSL object for real TLS.
    pub fn initWithSsl(alloc: Allocator, fd: posix.fd_t, ssl: *anyopaque, openssl: *OpenSslLib) TlsConnection {
        return .{
            .inner_fd = fd,
            .ssl_ptr = ssl,
            .openssl = openssl,
            .allocator = alloc,
        };
    }

    /// Perform the TLS handshake on this connection.
    /// For non-blocking sockets, may return WantRead or WantWrite — the caller
    /// should re-arm epoll and retry when the socket is ready.
    pub fn doHandshake(self: *TlsConnection) !void {
        const ssl = self.ssl_ptr orelse {
            log.err("TLS handshake attempted but no SSL object — TLS not configured.", .{});
            return error.TlsNotImplemented;
        };
        const ossl = self.openssl orelse return error.TlsNotImplemented;

        const ret = ossl.SSL_accept(ssl);
        if (ret == 1) {
            self.tls_established = true;
            log.info("TLS handshake completed on fd={d}", .{self.inner_fd});
            return;
        }

        const err = ossl.SSL_get_error(ssl, ret);
        switch (err) {
            OpenSslLib.SSL_ERROR_WANT_READ => return error.WantRead,
            OpenSslLib.SSL_ERROR_WANT_WRITE => return error.WantWrite,
            else => {
                const err_str = ossl.getErrorString();
                const err_msg: []const u8 = std.mem.sliceTo(&err_str, 0);
                log.err("TLS handshake failed on fd={d}: {s}", .{ self.inner_fd, err_msg });
                return error.TlsHandshakeFailed;
            },
        }
    }

    /// Read decrypted data from the TLS connection.
    pub fn read(self: *TlsConnection, buf: []u8) !usize {
        const ssl = self.ssl_ptr orelse {
            if (!self.tls_established) return error.TlsNotEstablished;
            return posix.recv(self.inner_fd, buf, 0) catch return error.ReadError;
        };
        const ossl = self.openssl orelse return error.TlsNotEstablished;

        const len: c_int = @intCast(@min(buf.len, std.math.maxInt(c_int)));
        const ret = ossl.SSL_read(ssl, buf.ptr, len);
        if (ret > 0) return @intCast(ret);

        const err = ossl.SSL_get_error(ssl, ret);
        switch (err) {
            OpenSslLib.SSL_ERROR_WANT_READ => return error.WouldBlock,
            OpenSslLib.SSL_ERROR_WANT_WRITE => return error.WouldBlock,
            OpenSslLib.SSL_ERROR_ZERO_RETURN => return 0, // Clean shutdown
            else => return error.ReadError,
        }
    }

    /// Write encrypted data to the TLS connection.
    pub fn write(self: *TlsConnection, data: []const u8) !usize {
        const ssl = self.ssl_ptr orelse {
            if (!self.tls_established) return error.TlsNotEstablished;
            return posix.send(self.inner_fd, data, 0) catch return error.WriteError;
        };
        const ossl = self.openssl orelse return error.TlsNotEstablished;

        const len: c_int = @intCast(@min(data.len, std.math.maxInt(c_int)));
        const ret = ossl.SSL_write(ssl, data.ptr, len);
        if (ret > 0) return @intCast(ret);

        const err = ossl.SSL_get_error(ssl, ret);
        switch (err) {
            OpenSslLib.SSL_ERROR_WANT_WRITE => return error.WouldBlock,
            OpenSslLib.SSL_ERROR_WANT_READ => return error.WouldBlock,
            else => return error.WriteError,
        }
    }

    pub fn deinit(self: *TlsConnection) void {
        if (self.ssl_ptr) |ssl| {
            if (self.openssl) |ossl| {
                _ = ossl.SSL_shutdown(ssl);
                ossl.SSL_free(ssl);
            }
            self.ssl_ptr = null;
        }
    }
};

/// TLS context — shared across all connections.
/// Holds the server certificate, private key, and trusted CAs.
///
/// Wraps OpenSSL's SSL_CTX. Created once at broker startup, used to
/// create per-connection SSL objects via createSsl().
pub const TlsContext = struct {
    config: TlsConfig,
    initialized: bool = false,
    ssl_ctx: ?*anyopaque = null,
    openssl: ?OpenSslLib = null,
    allocator: Allocator,

    pub fn init(alloc: Allocator, config: TlsConfig) !TlsContext {
        var ctx = TlsContext{
            .config = config,
            .allocator = alloc,
        };

        if (config.needsTls()) {
            try config.validate();

            // Load OpenSSL at runtime
            var ossl = OpenSslLib.load() catch |err| {
                log.err("Cannot initialize TLS: OpenSSL not available ({s})", .{@errorName(err)});
                return error.TlsNotAvailable;
            };
            errdefer ossl.close();

            // Create SSL_CTX
            const method = ossl.TLS_server_method() orelse return error.TlsInitFailed;
            const ssl_ctx = ossl.SSL_CTX_new(method) orelse return error.TlsInitFailed;
            errdefer ossl.SSL_CTX_free(ssl_ctx);

            // Set protocol version constraints
            _ = ossl.setMinProtoVersion(ssl_ctx, config.min_tls_version.toOpenSsl());
            _ = ossl.setMaxProtoVersion(ssl_ctx, config.max_tls_version.toOpenSsl());

            // Load certificate chain
            if (config.cert_file) |cert_path| {
                const cert_z = std.fmt.allocPrintZ(alloc, "{s}", .{cert_path}) catch return error.OutOfMemory;
                defer alloc.free(cert_z);
                if (ossl.SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_z) != 1) {
                    const err_str = ossl.getErrorString();
                    log.err("Failed to load certificate: {s}", .{std.mem.sliceTo(&err_str, 0)});
                    return error.CertificateLoadFailed;
                }
                log.info("Loaded certificate: {s}", .{cert_path});
            }

            // Load private key
            if (config.key_file) |key_path| {
                const key_z = std.fmt.allocPrintZ(alloc, "{s}", .{key_path}) catch return error.OutOfMemory;
                defer alloc.free(key_z);
                if (ossl.SSL_CTX_use_PrivateKey_file(ssl_ctx, key_z, OpenSslLib.SSL_FILETYPE_PEM) != 1) {
                    const err_str = ossl.getErrorString();
                    log.err("Failed to load private key: {s}", .{std.mem.sliceTo(&err_str, 0)});
                    return error.PrivateKeyLoadFailed;
                }

                // Verify cert/key match
                if (ossl.SSL_CTX_check_private_key(ssl_ctx) != 1) {
                    log.err("Certificate and private key do not match", .{});
                    return error.CertKeyMismatch;
                }
                log.info("Loaded private key: {s}", .{key_path});
            }

            // Load CA certificates for client verification (mTLS)
            if (config.ca_file) |ca_path| {
                const ca_z = std.fmt.allocPrintZ(alloc, "{s}", .{ca_path}) catch return error.OutOfMemory;
                defer alloc.free(ca_z);
                if (ossl.SSL_CTX_load_verify_locations(ssl_ctx, ca_z, null) != 1) {
                    log.warn("Failed to load CA file: {s}", .{ca_path});
                }
            }

            // Set client certificate verification mode
            const verify_mode: c_int = switch (config.client_auth) {
                .none => OpenSslLib.SSL_VERIFY_NONE,
                .requested => OpenSslLib.SSL_VERIFY_PEER,
                .required => OpenSslLib.SSL_VERIFY_PEER | OpenSslLib.SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
            };
            ossl.SSL_CTX_set_verify(ssl_ctx, verify_mode, null);

            ctx.ssl_ctx = ssl_ctx;
            ctx.openssl = ossl;
            ctx.initialized = true;

            log.info("TLS context initialized (min={s}, max={s}, client_auth={s})", .{
                @tagName(config.min_tls_version),
                @tagName(config.max_tls_version),
                @tagName(config.client_auth),
            });
        }

        return ctx;
    }

    /// Create a new SSL object for an accepted connection.
    /// The SSL object is bound to the fd and ready for SSL_accept().
    pub fn createSsl(self: *TlsContext, fd: posix.fd_t) !*anyopaque {
        const ossl = &(self.openssl orelse return error.TlsNotInitialized);
        const ssl_ctx = self.ssl_ctx orelse return error.TlsNotInitialized;

        const ssl = ossl.SSL_new(ssl_ctx) orelse return error.SslCreateFailed;
        errdefer ossl.SSL_free(ssl);

        if (ossl.SSL_set_fd(ssl, @intCast(fd)) != 1) {
            return error.SslSetFdFailed;
        }

        return ssl;
    }

    /// Get a mutable reference to the OpenSSL library.
    pub fn getOpenSsl(self: *TlsContext) ?*OpenSslLib {
        if (self.openssl != null) return &self.openssl.?;
        return null;
    }

    pub fn deinit(self: *TlsContext) void {
        if (self.ssl_ctx) |ctx| {
            if (self.openssl) |*ossl| {
                ossl.SSL_CTX_free(ctx);
                ossl.close();
            }
        }
        self.ssl_ctx = null;
        self.openssl = null;
        self.initialized = false;
    }
};

/// Generate a self-signed certificate for development/testing.
pub fn generateSelfSignedCert(alloc: Allocator, output_dir: []const u8) !struct { cert: []const u8, key: []const u8 } {
    _ = alloc;
    _ = output_dir;
    return error.NotImplemented;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "TlsConfig defaults" {
    const config = TlsConfig{};
    try testing.expect(!config.needsTls());
    try testing.expect(!config.needsSasl());
    try testing.expectEqual(TlsConfig.SecurityProtocol.plaintext, config.protocol);
}

test "TlsConfig SSL protocol needs TLS" {
    const config = TlsConfig{ .protocol = .ssl };
    try testing.expect(config.needsTls());
    try testing.expect(!config.needsSasl());
}

test "TlsConfig SASL_SSL needs both" {
    const config = TlsConfig{ .protocol = .sasl_ssl };
    try testing.expect(config.needsTls());
    try testing.expect(config.needsSasl());
}

test "TlsConfig SASL_PLAINTEXT needs only SASL" {
    const config = TlsConfig{ .protocol = .sasl_plaintext };
    try testing.expect(!config.needsTls());
    try testing.expect(config.needsSasl());
}

test "TlsConfig validate requires cert for SSL" {
    const config = TlsConfig{ .protocol = .ssl };
    try testing.expectError(error.NoCertificateConfigured, config.validate());
}

test "TlsConfig validate ok with cert" {
    const config = TlsConfig{
        .protocol = .ssl,
        .cert_file = "/path/to/cert.pem",
        .key_file = "/path/to/key.pem",
    };
    try config.validate();
}

test "TlsConfig validate ok for plaintext" {
    const config = TlsConfig{ .protocol = .plaintext };
    try config.validate();
}

test "TlsContext init plaintext" {
    const config = TlsConfig{};
    var ctx = try TlsContext.init(testing.allocator, config);
    defer ctx.deinit();
    try testing.expect(!ctx.initialized);
}

test "TlsConnection init" {
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();
    try testing.expect(!conn.tls_established);
    try testing.expectEqual(@as(posix.fd_t, 42), conn.inner_fd);
}

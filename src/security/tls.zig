const std = @import("std");
const testing = std.testing;
const posix = std.posix;
const log = std.log.scoped(.tls);
const Allocator = std.mem.Allocator;

/// TLS configuration for the broker.
///
/// Supports both client-facing (data plane) and inter-broker (control plane)
/// TLS connections. Uses OpenSSL/BoringSSL via C FFI for production, or
/// Zig's std.crypto for basic TLS operations.
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
/// Wraps the raw TCP socket with TLS encryption/decryption.
/// In a production implementation, this would use OpenSSL's SSL_read/SSL_write.
/// For now, provides the interface that the network layer needs.
pub const TlsConnection = struct {
    inner_fd: posix.fd_t,
    tls_established: bool = false,
    peer_cert_subject: ?[]const u8 = null,
    allocator: Allocator,

    pub fn init(alloc: Allocator, fd: posix.fd_t) TlsConnection {
        return .{
            .inner_fd = fd,
            .allocator = alloc,
        };
    }

    /// Perform the TLS handshake on this connection.
    /// Returns error.TlsNotImplemented instead of silently pretending TLS works.
    /// A production implementation would use OpenSSL FFI (SSL_do_handshake).
    /// Zig's std.crypto provides TLS 1.3 primitives but not a full server-side
    /// TLS implementation, so this is a documented limitation.
    pub fn doHandshake(self: *TlsConnection) !void {
        _ = self;
        log.err("TLS handshake attempted but TLS is not implemented. " ++
            "Use PLAINTEXT or SASL_PLAINTEXT protocol, or provide an OpenSSL-backed build.", .{});
        return error.TlsNotImplemented;
    }

    /// Read decrypted data from the TLS connection.
    /// In a real implementation, this calls SSL_read().
    pub fn read(self: *TlsConnection, buf: []u8) !usize {
        if (!self.tls_established) return error.TlsNotEstablished;
        // Passthrough to raw socket for now
        return posix.recv(self.inner_fd, buf, 0) catch return error.ReadError;
    }

    /// Write encrypted data to the TLS connection.
    /// In a real implementation, this calls SSL_write().
    pub fn write(self: *TlsConnection, data: []const u8) !usize {
        if (!self.tls_established) return error.TlsNotEstablished;
        // Passthrough to raw socket for now
        return posix.send(self.inner_fd, data, 0) catch return error.WriteError;
    }

    pub fn deinit(self: *TlsConnection) void {
        // In real implementation: SSL_shutdown() + SSL_free()
        _ = self;
    }
};

/// TLS context — shared across all connections.
/// Holds the server certificate, private key, and trusted CAs.
///
/// In a real implementation, this wraps OpenSSL's SSL_CTX.
pub const TlsContext = struct {
    config: TlsConfig,
    initialized: bool = false,
    allocator: Allocator,

    pub fn init(alloc: Allocator, config: TlsConfig) !TlsContext {
        var ctx = TlsContext{
            .config = config,
            .allocator = alloc,
        };

        if (config.needsTls()) {
            try config.validate();
            // TODO: Load certificates and initialize OpenSSL context
            // SSL_CTX_new(TLS_server_method())
            // SSL_CTX_use_certificate_chain_file()
            // SSL_CTX_use_PrivateKey_file()
            // SSL_CTX_load_verify_locations()
            ctx.initialized = true;
            log.info("TLS context initialized (min={s}, max={s})", .{
                @tagName(config.min_tls_version),
                @tagName(config.max_tls_version),
            });
        }

        return ctx;
    }

    /// Create a new TLS connection from an accepted TCP socket.
    /// Returns TlsNotImplemented for SSL/SASL_SSL protocols.
    /// SASL auth (SASL_PLAINTEXT) is wired through the handler's
    /// SaslHandshake + SaslAuthenticate handlers — no TLS needed.
    pub fn wrapConnection(self: *TlsContext, fd: posix.fd_t) !TlsConnection {
        if (!self.initialized) return error.TlsNotInitialized;
        var conn = TlsConnection.init(self.allocator, fd);
        try conn.doHandshake();
        return conn;
    }

    pub fn deinit(self: *TlsContext) void {
        // In real implementation: SSL_CTX_free()
        self.initialized = false;
    }
};

/// Generate a self-signed certificate for development/testing.
/// Returns paths to the generated cert and key files.
pub fn generateSelfSignedCert(alloc: Allocator, output_dir: []const u8) !struct { cert: []const u8, key: []const u8 } {
    _ = alloc;
    _ = output_dir;
    // TODO: Use openssl CLI or C FFI to generate self-signed cert
    // openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
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
    try testing.expect(!ctx.initialized); // Not needed for plaintext
}

test "TlsConnection init" {
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();
    try testing.expect(!conn.tls_established);
    try testing.expectEqual(@as(posix.fd_t, 42), conn.inner_fd);
}

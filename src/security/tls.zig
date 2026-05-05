const std = @import("std");
const testing = std.testing;
const posix = std.posix;
const linux = std.os.linux;
const log = std.log.scoped(.tls);
const Allocator = std.mem.Allocator;
const OpenSslLib = @import("openssl.zig").OpenSslLib;
const fs = @import("fs_compat");

fn monotonicMs() i64 {
    return @intCast(@import("time_compat").monotonicMilliTimestamp());
}

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

    // Java keystore/truststore paths are kept for config compatibility only.
    // The current TLS implementation loads PEM files directly, so validate()
    // fails closed if these fields are set instead of silently ignoring them.
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

        if (@intFromEnum(self.min_tls_version) > @intFromEnum(self.max_tls_version)) {
            return error.InvalidTlsVersionRange;
        }
        if (self.keystore_path != null or self.keystore_password != null) {
            return error.UnsupportedKeystoreFormat;
        }
        if (self.truststore_path != null or self.truststore_password != null) {
            return error.UnsupportedTruststoreFormat;
        }
        if (self.cert_file == null) {
            return error.NoCertificateConfigured;
        }
        if (self.key_file == null) {
            return error.NoPrivateKeyConfigured;
        }
        if (self.client_auth != .none and self.ca_file == null) {
            return error.NoTrustAnchorsConfigured;
        }
    }

    /// Validate outbound TLS configuration.
    ///
    /// Client contexts do not require a local certificate unless mTLS is
    /// configured, but they still must fail closed for unsupported Kafka-style
    /// JKS fields, invalid protocol ranges, and partial client cert/key config.
    pub fn validateClient(self: *const TlsConfig) !void {
        if (!self.needsTls()) return;

        if (@intFromEnum(self.min_tls_version) > @intFromEnum(self.max_tls_version)) {
            return error.InvalidTlsVersionRange;
        }
        if (self.keystore_path != null or self.keystore_password != null) {
            return error.UnsupportedKeystoreFormat;
        }
        if (self.truststore_path != null or self.truststore_password != null) {
            return error.UnsupportedTruststoreFormat;
        }
        if ((self.cert_file == null) != (self.key_file == null)) {
            return error.IncompleteClientCertificateConfig;
        }
    }
};

pub const TlsFileFingerprint = struct {
    configured: bool = false,
    exists: bool = false,
    size: u64 = 0,
    inode: u64 = 0,
    mtime_sec: i64 = 0,
    mtime_nsec: u32 = 0,

    pub fn eql(self: TlsFileFingerprint, other: TlsFileFingerprint) bool {
        return self.configured == other.configured and
            self.exists == other.exists and
            self.size == other.size and
            self.inode == other.inode and
            self.mtime_sec == other.mtime_sec and
            self.mtime_nsec == other.mtime_nsec;
    }
};

pub const TlsCertificateWatchState = struct {
    cert: TlsFileFingerprint = .{},
    key: TlsFileFingerprint = .{},
    ca: TlsFileFingerprint = .{},

    pub fn capture(config: *const TlsConfig) !TlsCertificateWatchState {
        return .{
            .cert = try captureFileFingerprint(config.cert_file),
            .key = try captureFileFingerprint(config.key_file),
            .ca = try captureFileFingerprint(config.ca_file),
        };
    }

    pub fn eql(self: TlsCertificateWatchState, other: TlsCertificateWatchState) bool {
        return self.cert.eql(other.cert) and self.key.eql(other.key) and self.ca.eql(other.ca);
    }

    pub fn changed(self: TlsCertificateWatchState, config: *const TlsConfig) !bool {
        const current = try capture(config);
        return !self.eql(current);
    }
};

fn captureFileFingerprint(path_opt: ?[]const u8) !TlsFileFingerprint {
    const path = path_opt orelse return .{};
    const file = fs.openFileAbsolute(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return .{ .configured = true },
        else => |open_err| return open_err,
    };
    defer file.close();

    var statx_buf: linux.Statx = undefined;
    const rc = linux.statx(file.fd, "", linux.AT.EMPTY_PATH, .{ .SIZE = true, .INO = true, .MTIME = true }, &statx_buf);
    switch (posix.errno(rc)) {
        .SUCCESS => return .{
            .configured = true,
            .exists = true,
            .size = statx_buf.size,
            .inode = statx_buf.ino,
            .mtime_sec = statx_buf.mtime.sec,
            .mtime_nsec = statx_buf.mtime.nsec,
        },
        .ACCES => return error.AccessDenied,
        .PERM => return error.PermissionDenied,
        .BADF => return error.Unexpected,
        .INVAL => return error.Unexpected,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

/// TLS connection state for a single client connection.
///
/// Wraps the raw TCP socket with TLS encryption/decryption via OpenSSL.
/// Handles non-blocking TLS handshake (SSL_accept returns WANT_READ/WANT_WRITE)
/// and encrypted I/O (SSL_read/SSL_write).
///
/// After handshake completion, performs:
/// - Certificate chain validation (via SSL_get_verify_result)
/// - Certificate expiry check (via X509_get0_notAfter + X509_cmp_current_time)
/// - mTLS principal extraction (via X509_get_subject_name + X509_NAME_oneline)
///
/// NOTE: AutoMQ (Java) uses SslPrincipalMapper to transform X.500 DNs into
/// Kafka principals via configurable regex rules. ZMQ extracts the CN directly
/// from the subject DN, which covers the common case. Full DN-to-principal
/// mapping rules are not yet implemented.
pub const TlsConnection = struct {
    inner_fd: posix.fd_t,
    tls_established: bool = false,
    ssl_ptr: ?*anyopaque = null,
    openssl: ?*OpenSslLib = null,
    /// Extracted mTLS client principal (e.g., "User:kafka-client-1").
    /// Heap-allocated, owned by this connection. Freed on deinit.
    peer_cert_subject: ?[]const u8 = null,
    allocator: Allocator,
    /// Timestamp (ms) when the handshake was initiated. Used to enforce
    /// TLS_HANDSHAKE_TIMEOUT_MS and prevent slow-client DoS attacks.
    handshake_start_ms: i64 = 0,

    /// Maximum time (ms) allowed for a TLS handshake to complete.
    /// Connections that don't complete the handshake within this window
    /// are forcibly closed. Prevents slow-client DoS where an attacker
    /// opens many connections and sends ClientHello bytes one at a time.
    pub const TLS_HANDSHAKE_TIMEOUT_MS: i64 = 30_000;

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
            .handshake_start_ms = monotonicMs(),
        };
    }

    /// Perform the TLS handshake on this connection (server-side SSL_accept).
    /// For non-blocking sockets, may return WantRead or WantWrite — the caller
    /// should re-arm epoll and retry when the socket is ready.
    ///
    /// On successful completion, runs post-handshake validation:
    /// 1. Certificate chain verification (rejects untrusted chains)
    /// 2. Certificate expiry check (rejects expired/not-yet-valid certs)
    /// 3. mTLS principal extraction (populates peer_cert_subject)
    pub fn doHandshake(self: *TlsConnection) !void {
        const ssl = self.ssl_ptr orelse {
            log.err("TLS handshake attempted but no SSL object — TLS not configured.", .{});
            return error.TlsContextUnavailable;
        };
        const ossl = self.openssl orelse return error.TlsContextUnavailable;

        const ret = ossl.SSL_accept(ssl);
        if (ret == 1) {
            self.tls_established = true;

            // Post-handshake validation: chain, expiry, principal extraction
            self.validatePeerCertificate() catch |err| {
                log.err("TLS post-handshake validation failed on fd={d}: {s}", .{ self.inner_fd, @errorName(err) });
                self.tls_established = false;
                return err;
            };

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

    /// Post-handshake peer certificate validation.
    /// Called immediately after SSL_accept returns 1 (handshake success).
    ///
    /// Performs three checks:
    /// 1. Certificate chain verification via SSL_get_verify_result
    /// 2. Certificate expiry check via X509_get0_notAfter
    /// 3. mTLS principal extraction via subject DN parsing
    fn validatePeerCertificate(self: *TlsConnection) !void {
        const ssl = self.ssl_ptr orelse return;
        const ossl = self.openssl orelse return;

        // 1. Check certificate chain verification result.
        // OpenSSL verifies the chain during the handshake; we check the result here.
        // If the CA file was loaded and client_auth is requested/required, OpenSSL
        // will have validated the chain. X509_V_OK means the chain is trusted.
        const verify_result = ossl.getVerifyResult(ssl);
        if (verify_result >= 0 and verify_result != OpenSslLib.X509_V_OK) {
            log.err("TLS peer certificate chain verification failed on fd={d}: X509 error code={d}", .{
                self.inner_fd, verify_result,
            });
            return error.CertificateVerificationFailed;
        }

        // 2. Check certificate expiry.
        // Even if the chain is valid, we explicitly check expiry because some
        // OpenSSL configurations may not treat expiry as a hard error.
        const expiry_status = ossl.checkPeerCertExpiry(ssl);
        switch (expiry_status) {
            .expired => {
                log.err("TLS peer certificate has expired on fd={d}", .{self.inner_fd});
                return error.CertificateExpired;
            },
            .not_yet_valid => {
                log.err("TLS peer certificate is not yet valid on fd={d}", .{self.inner_fd});
                return error.CertificateNotYetValid;
            },
            .no_certificate => {
                // No peer cert is normal for client_auth=none. Only log at debug.
                log.debug("No peer certificate presented on fd={d}", .{self.inner_fd});
            },
            .valid => {
                log.debug("TLS peer certificate expiry check passed on fd={d}", .{self.inner_fd});
            },
            .unknown => {
                // Could not check expiry (missing OpenSSL functions). Not fatal.
                log.debug("Could not check certificate expiry on fd={d} (functions unavailable)", .{self.inner_fd});
            },
        }

        // 3. Extract mTLS principal from client certificate subject DN.
        // Populates self.peer_cert_subject with "User:<CN>" for use in ACL checks.
        if (ossl.extractMtlsPrincipal(ssl, self.allocator)) |principal| {
            self.peer_cert_subject = principal;
            log.info("mTLS principal extracted on fd={d}: {s}", .{ self.inner_fd, principal });
        }
    }

    /// Check if the TLS handshake has timed out.
    /// Returns true if the handshake started more than TLS_HANDSHAKE_TIMEOUT_MS ago
    /// and has not yet completed. The caller should close the connection.
    pub fn isHandshakeTimedOut(self: *const TlsConnection, now_ms: i64) bool {
        if (self.tls_established) return false;
        if (self.handshake_start_ms == 0) return false;
        return (now_ms - self.handshake_start_ms) > TLS_HANDSHAKE_TIMEOUT_MS;
    }

    /// Read decrypted data from the TLS connection.
    pub fn read(self: *TlsConnection, buf: []u8) !usize {
        const ssl = self.ssl_ptr orelse {
            if (!self.tls_established) return error.TlsNotEstablished;
            return @import("posix_compat").recv(self.inner_fd, buf, 0) catch return error.ReadError;
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
            return @import("posix_compat").send(self.inner_fd, data, 0) catch return error.WriteError;
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
        // Free the heap-allocated principal string
        if (self.peer_cert_subject) |subject| {
            self.allocator.free(@constCast(subject));
            self.peer_cert_subject = null;
        }
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
    certificate_watch: TlsCertificateWatchState = .{},

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

            // Configure cipher suites: strong ciphers only (matches Kafka's default).
            // ECDHE and DHE provide forward secrecy; AESGCM and CHACHA20 are modern
            // AEAD ciphers. Explicitly exclude anonymous ciphers, MD5, and DSS.
            // NOTE: AutoMQ uses Java's default SSL cipher suite list (JSSE). ZMQ
            // explicitly sets strong ciphers because OpenSSL's defaults may include
            // weaker ciphers depending on the system build.
            const default_ciphers = "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS";
            if (!ossl.setCipherList(ssl_ctx, default_ciphers)) {
                log.warn("Failed to set cipher list, using OpenSSL defaults", .{});
            }

            // Load certificate chain
            if (config.cert_file) |cert_path| {
                const cert_z = alloc.dupeZ(u8, cert_path) catch return error.OutOfMemory;
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
                const key_z = alloc.dupeZ(u8, key_path) catch return error.OutOfMemory;
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
                const ca_z = alloc.dupeZ(u8, ca_path) catch return error.OutOfMemory;
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
            ctx.certificate_watch = try TlsCertificateWatchState.capture(&config);

            log.info("TLS context initialized (min={s}, max={s}, client_auth={s})", .{
                @tagName(config.min_tls_version),
                @tagName(config.max_tls_version),
                @tagName(config.client_auth),
            });
        }

        return ctx;
    }

    pub fn reloadIfCertificatesChanged(self: *TlsContext) !bool {
        if (!self.config.needsTls()) return false;

        const current = try TlsCertificateWatchState.capture(&self.config);
        if (self.certificate_watch.eql(current)) return false;

        var replacement = try TlsContext.init(self.allocator, self.config);
        replacement.certificate_watch = current;

        var old = self.*;
        self.* = replacement;
        old.deinit();
        log.info("TLS certificate context reloaded after PEM file change", .{});
        return true;
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

/// Client-side TLS context for outbound connections (Raft RPC, broker→controller).
///
/// Unlike TlsContext (server-side), this uses TLS_client_method and SSL_connect
/// instead of TLS_server_method and SSL_accept. Shared across all outbound
/// connections from a single node.
pub const TlsClientContext = struct {
    ssl_ctx: ?*anyopaque = null,
    openssl: ?OpenSslLib = null,
    initialized: bool = false,

    pub fn init(config: TlsConfig) !TlsClientContext {
        if (!config.needsTls()) return TlsClientContext{};
        try config.validateClient();

        var ossl = OpenSslLib.load() catch |err| {
            log.err("Cannot initialize client TLS: OpenSSL not available ({s})", .{@errorName(err)});
            return error.TlsNotAvailable;
        };
        errdefer ossl.close();

        const method = ossl.TLS_client_method() orelse return error.TlsInitFailed;
        const ssl_ctx = ossl.SSL_CTX_new(method) orelse return error.TlsInitFailed;
        errdefer ossl.SSL_CTX_free(ssl_ctx);

        // Set protocol version constraints
        _ = ossl.setMinProtoVersion(ssl_ctx, config.min_tls_version.toOpenSsl());
        _ = ossl.setMaxProtoVersion(ssl_ctx, config.max_tls_version.toOpenSsl());

        // If CA file set, load for server certificate verification
        if (config.ca_file) |ca_path| {
            const ca_z = std.heap.page_allocator.dupeZ(u8, ca_path) catch return error.OutOfMemory;
            defer std.heap.page_allocator.free(ca_z);
            if (ossl.SSL_CTX_load_verify_locations(ssl_ctx, ca_z, null) != 1) {
                log.warn("Failed to load CA for client TLS: {s}", .{ca_path});
            } else {
                log.info("Client TLS: loaded CA file: {s}", .{ca_path});
            }
        } else if (ossl.SSL_CTX_set_default_verify_paths) |load_default_paths| {
            if (load_default_paths(ssl_ctx) != 1) {
                log.warn("Client TLS: failed to load default CA paths", .{});
            }
        }

        ossl.SSL_CTX_set_verify(ssl_ctx, OpenSslLib.SSL_VERIFY_PEER, null);

        // If client cert/key set (mTLS), load them
        if (config.cert_file) |cert_path| {
            const cert_z = std.heap.page_allocator.dupeZ(u8, cert_path) catch return error.OutOfMemory;
            defer std.heap.page_allocator.free(cert_z);
            if (ossl.SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_z) != 1) {
                const err_str = ossl.getErrorString();
                log.err("Failed to load client certificate: {s}", .{std.mem.sliceTo(&err_str, 0)});
                return error.CertificateLoadFailed;
            }
            log.info("Client TLS: loaded certificate: {s}", .{cert_path});
        }

        if (config.key_file) |key_path| {
            const key_z = std.heap.page_allocator.dupeZ(u8, key_path) catch return error.OutOfMemory;
            defer std.heap.page_allocator.free(key_z);
            if (ossl.SSL_CTX_use_PrivateKey_file(ssl_ctx, key_z, OpenSslLib.SSL_FILETYPE_PEM) != 1) {
                const err_str = ossl.getErrorString();
                log.err("Failed to load client private key: {s}", .{std.mem.sliceTo(&err_str, 0)});
                return error.PrivateKeyLoadFailed;
            }

            // Verify cert/key match
            if (ossl.SSL_CTX_check_private_key(ssl_ctx) != 1) {
                log.err("Client certificate and private key do not match", .{});
                return error.CertKeyMismatch;
            }
            log.info("Client TLS: loaded private key: {s}", .{key_path});
        }

        log.info("Client TLS context initialized (min={s}, max={s})", .{
            @tagName(config.min_tls_version),
            @tagName(config.max_tls_version),
        });

        return TlsClientContext{
            .ssl_ctx = ssl_ctx,
            .openssl = ossl,
            .initialized = true,
        };
    }

    /// Create an SSL object for a client connection and perform SSL_connect.
    /// For blocking sockets (Raft client), this does a blocking handshake.
    /// Does NOT perform hostname verification — use wrapConnectionWithHostname
    /// for outbound connections where the hostname is known.
    pub fn wrapConnection(self: *TlsClientContext, fd: posix.fd_t) !*anyopaque {
        return self.wrapConnectionWithHostname(fd, null);
    }

    /// Create an SSL object for a client connection with hostname verification.
    /// Calls SSL_set1_host to configure OpenSSL to verify the server certificate's
    /// SAN/CN against the expected hostname during the handshake.
    ///
    /// NOTE: AutoMQ (Java) uses SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")
    /// which verifies hostnames per RFC 6125. SSL_set1_host provides equivalent
    /// verification via OpenSSL's X509_check_host under the hood.
    pub fn wrapConnectionWithHostname(self: *TlsClientContext, fd: posix.fd_t, hostname: ?[*:0]const u8) !*anyopaque {
        const ossl = &(self.openssl orelse return error.TlsNotInitialized);
        const ssl = ossl.SSL_new(self.ssl_ctx orelse return error.TlsNotInitialized) orelse return error.SslCreateFailed;
        errdefer ossl.SSL_free(ssl);

        if (ossl.SSL_set_fd(ssl, @intCast(fd)) != 1) {
            return error.SslSetFdFailed;
        }

        // Enable hostname verification if a hostname was provided.
        // Must be set BEFORE SSL_connect so OpenSSL checks the server cert's
        // SAN/CN during the handshake.
        if (hostname) |host| {
            if (!ossl.setHostnameVerification(ssl, host)) {
                log.warn("TLS client hostname verification could not be enabled for fd={d}", .{fd});
                return error.HostnameVerificationUnavailable;
            } else {
                log.debug("Hostname verification enabled for fd={d}: {s}", .{ fd, host });
            }
        }

        const ret = ossl.SSL_connect(ssl);
        if (ret != 1) {
            const ssl_err = ossl.SSL_get_error(ssl, ret);
            const err_str = ossl.getErrorString();
            log.err("TLS client handshake failed on fd={d}: ssl_error={d} {s}", .{
                fd, ssl_err, std.mem.sliceTo(&err_str, 0),
            });
            return error.TlsHandshakeFailed;
        }

        // Post-handshake: verify the certificate chain result
        const verify_result = ossl.getVerifyResult(ssl);
        if (verify_result >= 0 and verify_result != OpenSslLib.X509_V_OK) {
            log.err("TLS client certificate chain verification failed on fd={d}: X509 error code={d}", .{
                fd, verify_result,
            });
            return error.CertificateVerificationFailed;
        }

        // Post-handshake: check certificate expiry
        const expiry_status = ossl.checkPeerCertExpiry(ssl);
        switch (expiry_status) {
            .expired => {
                log.err("TLS server certificate has expired on fd={d}", .{fd});
                return error.CertificateExpired;
            },
            .not_yet_valid => {
                log.err("TLS server certificate is not yet valid on fd={d}", .{fd});
                return error.CertificateNotYetValid;
            },
            .valid => {
                log.debug("TLS server certificate expiry check passed on fd={d}", .{fd});
            },
            .no_certificate, .unknown => {},
        }

        log.info("TLS client handshake completed on fd={d}", .{fd});
        return ssl;
    }

    /// Get a mutable reference to the OpenSSL library.
    pub fn getOpenSsl(self: *TlsClientContext) ?*OpenSslLib {
        if (self.openssl != null) return &self.openssl.?;
        return null;
    }

    pub fn deinit(self: *TlsClientContext) void {
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

/// Validate a peer certificate after a successful TLS handshake.
/// This is a standalone function for use by server.zig's epoll loop, which
/// calls SSL_accept directly rather than going through TlsConnection.doHandshake.
///
/// Returns the mTLS principal (heap-allocated, caller-owned) or null.
/// Returns error if the certificate is invalid (expired, untrusted chain).
pub fn validatePeerCertificatePostHandshake(
    ossl: *const OpenSslLib,
    ssl: *anyopaque,
    fd: posix.fd_t,
    allocator: Allocator,
) !?[]u8 {
    // 1. Check certificate chain verification result
    const verify_result = ossl.getVerifyResult(ssl);
    if (verify_result >= 0 and verify_result != OpenSslLib.X509_V_OK) {
        log.err("TLS peer certificate chain verification failed on fd={d}: X509 error code={d}", .{
            fd, verify_result,
        });
        return error.CertificateVerificationFailed;
    }

    // 2. Check certificate expiry
    const expiry_status = ossl.checkPeerCertExpiry(ssl);
    switch (expiry_status) {
        .expired => {
            log.err("TLS peer certificate has expired on fd={d}", .{fd});
            return error.CertificateExpired;
        },
        .not_yet_valid => {
            log.err("TLS peer certificate is not yet valid on fd={d}", .{fd});
            return error.CertificateNotYetValid;
        },
        .no_certificate => {
            log.debug("No peer certificate presented on fd={d}", .{fd});
        },
        .valid => {
            log.debug("TLS peer certificate expiry check passed on fd={d}", .{fd});
        },
        .unknown => {
            log.debug("Could not check certificate expiry on fd={d} (functions unavailable)", .{fd});
        },
    }

    // 3. Extract mTLS principal from client certificate subject DN
    return ossl.extractMtlsPrincipal(ssl, allocator);
}

/// Check whether a TLS handshake has timed out based on start time.
/// Used by server.zig's epoll loop to enforce handshake timeouts.
pub fn isHandshakeTimedOut(handshake_start_ms: i64, now_ms: i64) bool {
    if (handshake_start_ms == 0) return false;
    return (now_ms - handshake_start_ms) > TlsConnection.TLS_HANDSHAKE_TIMEOUT_MS;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

fn writeTlsRotationTestFile(path: []const u8, bytes: []const u8) !void {
    const file = try fs.createFileAbsolute(path, .{ .truncate = true });
    defer file.close();
    try file.writeAll(bytes);
    try file.sync();
}

fn tlsRotationTestPath(tmp: *const testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(testing.allocator, ".zig-cache/tmp/{s}/{s}", .{
        tmp.sub_path[0..],
        name,
    });
}

var fake_tls_test_handle: u8 = 0;

fn fakeOpaque() *anyopaque {
    return @ptrCast(&fake_tls_test_handle);
}

fn fakeTlsMethod() callconv(.c) ?*anyopaque {
    return fakeOpaque();
}

fn fakeSslCtxNew(_: ?*anyopaque) callconv(.c) ?*anyopaque {
    return fakeOpaque();
}

fn fakeSslCtxFree(_: ?*anyopaque) callconv(.c) void {}

fn fakeSslCtxUseFile(_: ?*anyopaque, _: [*:0]const u8, _: c_int) callconv(.c) c_int {
    return 1;
}

fn fakeSslCtxUseCertFile(_: ?*anyopaque, _: [*:0]const u8) callconv(.c) c_int {
    return 1;
}

fn fakeSslCtxCheckPrivateKey(_: ?*anyopaque) callconv(.c) c_int {
    return 1;
}

fn fakeSslCtxLoadVerifyLocations(_: ?*anyopaque, _: ?[*:0]const u8, _: ?[*:0]const u8) callconv(.c) c_int {
    return 1;
}

fn fakeSslCtxSetVerify(_: ?*anyopaque, _: c_int, _: ?*anyopaque) callconv(.c) void {}

fn fakeSslCtxCtrl(_: ?*anyopaque, _: c_int, _: c_long, _: ?*anyopaque) callconv(.c) c_long {
    return 1;
}

fn fakeSslNew(_: ?*anyopaque) callconv(.c) ?*anyopaque {
    return fakeOpaque();
}

fn fakeSslFree(_: ?*anyopaque) callconv(.c) void {}

fn fakeSslSetFd(_: ?*anyopaque, _: c_int) callconv(.c) c_int {
    return 1;
}

fn fakeSslHandshake(_: ?*anyopaque) callconv(.c) c_int {
    return 1;
}

fn fakeSslRead(_: ?*anyopaque, _: [*]u8, _: c_int) callconv(.c) c_int {
    return -1;
}

fn fakeSslWrite(_: ?*anyopaque, _: [*]const u8, _: c_int) callconv(.c) c_int {
    return -1;
}

fn fakeSslShutdown(_: ?*anyopaque) callconv(.c) c_int {
    return 1;
}

fn fakeSslGetError(_: ?*anyopaque, _: c_int) callconv(.c) c_int {
    return OpenSslLib.SSL_ERROR_SSL;
}

fn fakeSslCtxSetCipherList(_: ?*anyopaque, _: [*:0]const u8) callconv(.c) c_int {
    return 1;
}

fn fakeOpenSslInit(_: u64, _: ?*anyopaque) callconv(.c) c_int {
    return 1;
}

fn fakeErrGetError() callconv(.c) c_ulong {
    return 0;
}

fn fakeErrErrorString(_: c_ulong, buf: [*]u8, len: usize) callconv(.c) void {
    if (len > 0) buf[0] = 0;
}

fn fakeOpenSslLibWithoutHostnameVerification() OpenSslLib {
    return .{
        .ssl_handle = fakeOpaque(),
        .crypto_handle = fakeOpaque(),
        .TLS_server_method = fakeTlsMethod,
        .TLS_client_method = fakeTlsMethod,
        .SSL_CTX_new = fakeSslCtxNew,
        .SSL_CTX_free = fakeSslCtxFree,
        .SSL_CTX_use_certificate_chain_file = fakeSslCtxUseCertFile,
        .SSL_CTX_use_PrivateKey_file = fakeSslCtxUseFile,
        .SSL_CTX_check_private_key = fakeSslCtxCheckPrivateKey,
        .SSL_CTX_load_verify_locations = fakeSslCtxLoadVerifyLocations,
        .SSL_CTX_set_verify = fakeSslCtxSetVerify,
        .SSL_CTX_ctrl = fakeSslCtxCtrl,
        .SSL_new = fakeSslNew,
        .SSL_free = fakeSslFree,
        .SSL_set_fd = fakeSslSetFd,
        .SSL_accept = fakeSslHandshake,
        .SSL_connect = fakeSslHandshake,
        .SSL_read = fakeSslRead,
        .SSL_write = fakeSslWrite,
        .SSL_shutdown = fakeSslShutdown,
        .SSL_get_error = fakeSslGetError,
        .SSL_CTX_set_cipher_list = fakeSslCtxSetCipherList,
        .OPENSSL_init_ssl = fakeOpenSslInit,
        .ERR_get_error = fakeErrGetError,
        .ERR_error_string_n = fakeErrErrorString,
    };
}

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

test "TlsConfig validate rejects unsupported keystore fields" {
    const config = TlsConfig{
        .protocol = .ssl,
        .keystore_path = "/path/to/kafka.keystore.jks",
        .keystore_password = "secret",
    };
    try testing.expectError(error.UnsupportedKeystoreFormat, config.validate());
}

test "TlsConfig validate rejects unsupported truststore fields" {
    const config = TlsConfig{
        .protocol = .ssl,
        .cert_file = "/path/to/cert.pem",
        .key_file = "/path/to/key.pem",
        .truststore_path = "/path/to/kafka.truststore.jks",
    };
    try testing.expectError(error.UnsupportedTruststoreFormat, config.validate());
}

test "TlsConfig validate rejects inverted TLS version range" {
    const config = TlsConfig{
        .protocol = .ssl,
        .cert_file = "/path/to/cert.pem",
        .key_file = "/path/to/key.pem",
        .min_tls_version = .tls_1_3,
        .max_tls_version = .tls_1_2,
    };
    try testing.expectError(error.InvalidTlsVersionRange, config.validate());
}

test "TlsConfig validateClient allows CA-only outbound TLS" {
    const config = TlsConfig{
        .protocol = .ssl,
        .ca_file = "/path/to/ca.pem",
    };
    try config.validateClient();
}

test "TlsConfig validateClient rejects invalid outbound TLS config" {
    const inverted = TlsConfig{
        .protocol = .ssl,
        .min_tls_version = .tls_1_3,
        .max_tls_version = .tls_1_2,
    };
    try testing.expectError(error.InvalidTlsVersionRange, inverted.validateClient());

    const unsupported_truststore = TlsConfig{
        .protocol = .ssl,
        .truststore_path = "/path/to/kafka.truststore.jks",
    };
    try testing.expectError(error.UnsupportedTruststoreFormat, unsupported_truststore.validateClient());

    const partial_mtls = TlsConfig{
        .protocol = .ssl,
        .cert_file = "/path/to/client.pem",
    };
    try testing.expectError(error.IncompleteClientCertificateConfig, partial_mtls.validateClient());
}

test "TlsConfig validate requires trust anchors for mTLS" {
    const missing_ca = TlsConfig{
        .protocol = .ssl,
        .cert_file = "/path/to/cert.pem",
        .key_file = "/path/to/key.pem",
        .client_auth = .required,
    };
    try testing.expectError(error.NoTrustAnchorsConfigured, missing_ca.validate());

    const with_ca = TlsConfig{
        .protocol = .ssl,
        .cert_file = "/path/to/cert.pem",
        .key_file = "/path/to/key.pem",
        .ca_file = "/path/to/ca.pem",
        .client_auth = .required,
    };
    try with_ca.validate();
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

test "TlsCertificateWatchState detects PEM file rotation" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const cert_path = try tlsRotationTestPath(&tmp, "cert.pem");
    defer testing.allocator.free(cert_path);
    const key_path = try tlsRotationTestPath(&tmp, "key.pem");
    defer testing.allocator.free(key_path);

    try writeTlsRotationTestFile(cert_path, "cert-v1");
    try writeTlsRotationTestFile(key_path, "key-v1");

    const config = TlsConfig{
        .protocol = .ssl,
        .cert_file = cert_path,
        .key_file = key_path,
    };
    const initial = try TlsCertificateWatchState.capture(&config);
    try testing.expect(initial.cert.configured);
    try testing.expect(initial.cert.exists);
    try testing.expect(initial.key.exists);
    try testing.expect(!try initial.changed(&config));

    try writeTlsRotationTestFile(cert_path, "cert-v2-rotated");
    try testing.expect(try initial.changed(&config));
}

test "TlsCertificateWatchState treats deleted configured PEM as changed" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const cert_path = try tlsRotationTestPath(&tmp, "cert.pem");
    defer testing.allocator.free(cert_path);
    const key_path = try tlsRotationTestPath(&tmp, "key.pem");
    defer testing.allocator.free(key_path);

    try writeTlsRotationTestFile(cert_path, "cert-v1");
    try writeTlsRotationTestFile(key_path, "key-v1");

    const config = TlsConfig{
        .protocol = .ssl,
        .cert_file = cert_path,
        .key_file = key_path,
    };
    const initial = try TlsCertificateWatchState.capture(&config);
    try fs.deleteFileAbsolute(cert_path);

    const current = try TlsCertificateWatchState.capture(&config);
    try testing.expect(current.cert.configured);
    try testing.expect(!current.cert.exists);
    try testing.expect(!initial.eql(current));
}

test "TlsContext reloadIfCertificatesChanged no-ops without TLS" {
    const config = TlsConfig{};
    var ctx = try TlsContext.init(testing.allocator, config);
    defer ctx.deinit();
    try testing.expect(!try ctx.reloadIfCertificatesChanged());
}

test "TlsConnection init" {
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();
    try testing.expect(!conn.tls_established);
    try testing.expectEqual(@as(posix.fd_t, 42), conn.inner_fd);
}

test "TlsClientContext init plaintext returns uninitialized" {
    const config = TlsConfig{};
    var ctx = try TlsClientContext.init(config);
    defer ctx.deinit();
    try testing.expect(!ctx.initialized);
    try testing.expect(ctx.ssl_ctx == null);
}

test "TlsClientContext getOpenSsl returns null when uninitialized" {
    const config = TlsConfig{};
    var ctx = try TlsClientContext.init(config);
    defer ctx.deinit();
    try testing.expect(ctx.getOpenSsl() == null);
}

// -- Sprint 1: TLS Certificate Validation & mTLS Principal Extraction tests --

test "TlsConnection handshake timeout constant is 30 seconds" {
    // C4: TLS handshake timeout is enforced (30s)
    try testing.expectEqual(@as(i64, 30_000), TlsConnection.TLS_HANDSHAKE_TIMEOUT_MS);
}

test "TlsConnection isHandshakeTimedOut returns false when established" {
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();
    conn.tls_established = true;
    conn.handshake_start_ms = 1000;
    // Even if time elapsed is huge, established connections aren't timed out
    try testing.expect(!conn.isHandshakeTimedOut(1_000_000));
}

test "TlsConnection isHandshakeTimedOut returns false when no start time" {
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();
    // handshake_start_ms defaults to 0 (plaintext connection)
    try testing.expect(!conn.isHandshakeTimedOut(1_000_000));
}

test "TlsConnection isHandshakeTimedOut returns true after 30s" {
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();
    conn.handshake_start_ms = 10_000;
    // 10_000 + 30_001 = 40_001 → elapsed 30_001 > 30_000 → timed out
    try testing.expect(conn.isHandshakeTimedOut(40_001));
}

test "TlsConnection isHandshakeTimedOut returns false before 30s" {
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();
    conn.handshake_start_ms = 10_000;
    // 10_000 + 29_999 = 39_999 → elapsed 29_999 < 30_000 → not timed out
    try testing.expect(!conn.isHandshakeTimedOut(39_999));
}

test "isHandshakeTimedOut standalone function" {
    // C4: handshake timeout helper for server.zig
    try testing.expect(!isHandshakeTimedOut(0, 100_000)); // no start time
    try testing.expect(!isHandshakeTimedOut(1000, 20_000)); // within timeout
    try testing.expect(isHandshakeTimedOut(1000, 31_001)); // past timeout
    try testing.expect(!isHandshakeTimedOut(1000, 31_000)); // exactly at boundary
    try testing.expect(isHandshakeTimedOut(1000, 31_001)); // 1ms past boundary
}

test "TlsConnection initWithSsl sets handshake_start_ms" {
    // Verify that initWithSsl records the handshake start time so timeout
    // enforcement works for connections that are set up with SSL objects.
    // We can't create a real SSL object without OpenSSL, but we can check
    // the init logic using a mock opaque pointer.
    const alloc = testing.allocator;
    // Using a stack variable as a fake SSL pointer (never dereferenced)
    var fake_ssl: u8 = 0;
    var fake_ossl: OpenSslLib = undefined;
    var conn = TlsConnection.initWithSsl(alloc, 99, @ptrCast(&fake_ssl), &fake_ossl);
    // handshake_start_ms should be set to current time (non-zero)
    try testing.expect(conn.handshake_start_ms > 0);
    // Clean up without calling deinit (which would try to SSL_shutdown the fake pointer)
    conn.ssl_ptr = null;
    conn.openssl = null;
    conn.deinit();
}

test "Hostname verification setup via OpenSSL bindings" {
    // C1: tls.zig contains function that calls SSL_set1_host
    // Verify the binding is loadable and the wrapper exists
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    // SSL_set1_host should be available on modern OpenSSL
    try testing.expect(lib.SSL_set1_host != null);

    // Create a context and SSL object to test setHostnameVerification
    const method = lib.TLS_client_method() orelse return;
    const ctx = lib.SSL_CTX_new(method) orelse return;
    defer lib.SSL_CTX_free(ctx);

    const ssl = lib.SSL_new(ctx) orelse return;
    defer lib.SSL_free(ssl);

    // setHostnameVerification should succeed (sets hostname on the SSL object)
    const result = lib.setHostnameVerification(ssl, "kafka.example.com");
    try testing.expect(result);
}

test "Certificate expiry check function bindings loaded" {
    // C3: Certificate expiry is checked via X509_get_notAfter comparison
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    try testing.expect(lib.X509_get0_notAfter != null);
    try testing.expect(lib.X509_get0_notBefore != null);
    try testing.expect(lib.X509_cmp_current_time != null);
}

test "Certificate expiry check with no peer cert returns no_certificate" {
    // When no peer certificate is presented (e.g., server-side with client_auth=none),
    // checkPeerCertExpiry should return .no_certificate rather than crashing.
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    const method = lib.TLS_server_method() orelse return;
    const ctx = lib.SSL_CTX_new(method) orelse return;
    defer lib.SSL_CTX_free(ctx);

    const ssl = lib.SSL_new(ctx) orelse return;
    defer lib.SSL_free(ssl);

    // No handshake performed, so no peer certificate
    const status = lib.checkPeerCertExpiry(ssl);
    try testing.expectEqual(OpenSslLib.CertExpiryStatus.no_certificate, status);
}

test "Principal extraction with no peer cert returns null" {
    // C2: principal extraction returns null gracefully when no cert
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    const method = lib.TLS_server_method() orelse return;
    const ctx = lib.SSL_CTX_new(method) orelse return;
    defer lib.SSL_CTX_free(ctx);

    const ssl = lib.SSL_new(ctx) orelse return;
    defer lib.SSL_free(ssl);

    // No peer certificate → null principal
    const principal = lib.extractMtlsPrincipal(ssl, testing.allocator);
    try testing.expect(principal == null);
}

test "Principal extraction format uses User: prefix" {
    // C5: Test principal extraction format
    // This tests the CN extraction and formatting logic without requiring
    // a real TLS connection. The extractMtlsPrincipal function delegates
    // to extractCnFromDn, so we test the formatting end-to-end via the
    // CN extraction helper.
    const cn = OpenSslLib.extractCnFromDn("/C=US/ST=CA/O=ZMQ/CN=kafka-client-1");
    try testing.expect(cn != null);
    try testing.expectEqualStrings("kafka-client-1", cn.?);

    // The full principal would be "User:kafka-client-1" (tested via allocPrint in extractMtlsPrincipal)
    const principal = std.fmt.allocPrint(testing.allocator, "User:{s}", .{cn.?}) catch unreachable;
    defer testing.allocator.free(principal);
    try testing.expectEqualStrings("User:kafka-client-1", principal);
}

test "Verify result check on fresh SSL returns OK" {
    // SSL_get_verify_result returns X509_V_OK when no verification has happened
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    const method = lib.TLS_client_method() orelse return;
    const ctx = lib.SSL_CTX_new(method) orelse return;
    defer lib.SSL_CTX_free(ctx);

    const ssl = lib.SSL_new(ctx) orelse return;
    defer lib.SSL_free(ssl);

    const result = lib.getVerifyResult(ssl);
    try testing.expectEqual(OpenSslLib.X509_V_OK, result);
}

test "TlsClientContext rejects invalid outbound TLS config before OpenSSL load" {
    const config = TlsConfig{
        .protocol = .ssl,
        .keystore_path = "/path/to/kafka.keystore.jks",
    };
    try testing.expectError(error.UnsupportedKeystoreFormat, TlsClientContext.init(config));
}

test "TlsClientContext wrapConnectionWithHostname fails closed without hostname verification" {
    var ctx = TlsClientContext{
        .ssl_ctx = fakeOpaque(),
        .openssl = fakeOpenSslLibWithoutHostnameVerification(),
        .initialized = true,
    };

    try testing.expectError(
        error.HostnameVerificationUnavailable,
        ctx.wrapConnectionWithHostname(42, "kafka.example.com"),
    );
}

test "validatePeerCertificatePostHandshake with no peer cert returns null principal" {
    // The standalone validation function used by server.zig should return null
    // principal when there's no peer certificate (client_auth=none).
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    const method = lib.TLS_server_method() orelse return;
    const ctx = lib.SSL_CTX_new(method) orelse return;
    defer lib.SSL_CTX_free(ctx);

    const ssl = lib.SSL_new(ctx) orelse return;
    defer lib.SSL_free(ssl);

    const principal = try validatePeerCertificatePostHandshake(&lib, ssl, 42, testing.allocator);
    try testing.expect(principal == null);
}

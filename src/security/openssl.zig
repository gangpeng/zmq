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
    TLS_server_method: *const fn () callconv(.c) ?*anyopaque,
    TLS_client_method: *const fn () callconv(.c) ?*anyopaque,
    SSL_CTX_new: *const fn (?*anyopaque) callconv(.c) ?*anyopaque,
    SSL_CTX_free: *const fn (?*anyopaque) callconv(.c) void,
    SSL_CTX_use_certificate_chain_file: *const fn (?*anyopaque, [*:0]const u8) callconv(.c) c_int,
    SSL_CTX_use_PrivateKey_file: *const fn (?*anyopaque, [*:0]const u8, c_int) callconv(.c) c_int,
    SSL_CTX_check_private_key: *const fn (?*anyopaque) callconv(.c) c_int,
    SSL_CTX_load_verify_locations: *const fn (?*anyopaque, ?[*:0]const u8, ?[*:0]const u8) callconv(.c) c_int,
    SSL_CTX_set_verify: *const fn (?*anyopaque, c_int, ?*anyopaque) callconv(.c) void,
    /// SSL_CTX_set_default_verify_paths(ctx) → int — load system CA paths.
    SSL_CTX_set_default_verify_paths: ?*const fn (?*anyopaque) callconv(.c) c_int = null,

    /// SSL_CTX_ctrl(ctx, cmd, larg, parg) → long — generic control function
    /// Used for set_min/max_proto_version (macros in OpenSSL headers)
    SSL_CTX_ctrl: *const fn (?*anyopaque, c_int, c_long, ?*anyopaque) callconv(.c) c_long,
    SSL_new: *const fn (?*anyopaque) callconv(.c) ?*anyopaque,
    SSL_free: *const fn (?*anyopaque) callconv(.c) void,
    SSL_set_fd: *const fn (?*anyopaque, c_int) callconv(.c) c_int,
    SSL_accept: *const fn (?*anyopaque) callconv(.c) c_int,
    SSL_connect: *const fn (?*anyopaque) callconv(.c) c_int,
    SSL_read: *const fn (?*anyopaque, [*]u8, c_int) callconv(.c) c_int,
    SSL_write: *const fn (?*anyopaque, [*]const u8, c_int) callconv(.c) c_int,
    SSL_shutdown: *const fn (?*anyopaque) callconv(.c) c_int,
    SSL_get_error: *const fn (?*anyopaque, c_int) callconv(.c) c_int,
    SSL_CTX_set_cipher_list: *const fn (?*anyopaque, [*:0]const u8) callconv(.c) c_int,

    // -- Peer certificate inspection (mTLS) --
    // Available for extracting client certificate subject when mTLS is enabled.
    // In OpenSSL 3.x, SSL_get_peer_certificate was renamed to SSL_get1_peer_certificate.
    // We load SSL_get1_peer_certificate first, falling back to SSL_get_peer_certificate.
    /// SSL_get1_peer_certificate(ssl) → X509* (caller must X509_free)
    SSL_get1_peer_certificate: ?*const fn (?*anyopaque) callconv(.c) ?*anyopaque = null,
    /// X509_get_subject_name(x509) → X509_NAME* (internal pointer, do NOT free)
    X509_get_subject_name: ?*const fn (?*anyopaque) callconv(.c) ?*anyopaque = null,
    /// X509_NAME_oneline(name, buf, size) → char* (NUL-terminated string in buf)
    X509_NAME_oneline: ?*const fn (?*anyopaque, [*]u8, c_int) callconv(.c) ?[*:0]u8 = null,
    /// X509_free(x509) — free an X509 object obtained from SSL_get1_peer_certificate
    X509_free: ?*const fn (?*anyopaque) callconv(.c) void = null,

    // -- Hostname verification & certificate validation --
    // SSL_set1_host(ssl, hostname) → int — enables hostname verification against
    // the peer certificate's SAN/CN. OpenSSL 1.1.0+.
    // NOTE: AutoMQ (Java) uses SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")
    // which verifies hostname against SANs per RFC 6125. SSL_set1_host provides
    // equivalent verification via OpenSSL's X509_check_host under the hood.
    SSL_set1_host: ?*const fn (?*anyopaque, [*:0]const u8) callconv(.c) c_int = null,
    /// SSL_set_hostflags(ssl, flags) — configure hostname check flags (e.g. partial wildcards)
    SSL_set_hostflags: ?*const fn (?*anyopaque, c_uint) callconv(.c) void = null,
    /// SSL_get_verify_result(ssl) → long — returns X509_V_OK (0) if chain verification passed
    SSL_get_verify_result: ?*const fn (?*anyopaque) callconv(.c) c_long = null,
    /// X509_get_notAfter(x509) → ASN1_TIME* — pointer to the certificate's expiry time.
    /// In OpenSSL 1.1.0+ this is X509_get0_notAfter (returns const internal pointer).
    X509_get0_notAfter: ?*const fn (?*anyopaque) callconv(.c) ?*anyopaque = null,
    /// X509_get_notBefore(x509) → ASN1_TIME* — pointer to the certificate's start time.
    X509_get0_notBefore: ?*const fn (?*anyopaque) callconv(.c) ?*anyopaque = null,
    /// X509_cmp_current_time(asn1_time) → int
    ///   < 0: asn1_time is before current time (expired for notAfter)
    ///   > 0: asn1_time is after current time (still valid for notAfter)
    ///   = 0: error
    X509_cmp_current_time: ?*const fn (?*anyopaque) callconv(.c) c_int = null,
    /// X509_get_issuer_name(x509) → X509_NAME* (internal pointer, do NOT free)
    X509_get_issuer_name: ?*const fn (?*anyopaque) callconv(.c) ?*anyopaque = null,
    /// X509_get_serialNumber(x509) → ASN1_INTEGER* (internal pointer, do NOT free)
    X509_get_serialNumber: ?*const fn (?*anyopaque) callconv(.c) ?*anyopaque = null,

    // -- libcrypto function pointers --
    OPENSSL_init_ssl: *const fn (u64, ?*anyopaque) callconv(.c) c_int,
    ERR_get_error: *const fn () callconv(.c) c_ulong,
    ERR_error_string_n: *const fn (c_ulong, [*]u8, usize) callconv(.c) void,

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

    /// X509_V_OK — certificate verification succeeded
    pub const X509_V_OK: c_long = 0;
    /// X509_V_ERR_CERT_HAS_EXPIRED
    pub const X509_V_ERR_CERT_HAS_EXPIRED: c_long = 10;
    /// X509_V_ERR_CERT_NOT_YET_VALID
    pub const X509_V_ERR_CERT_NOT_YET_VALID: c_long = 9;
    /// X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS — stricter hostname matching
    pub const X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS: c_uint = 0x4;

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

    /// Convenience: SSL_CTX_set_cipher_list wrapper
    pub fn setCipherList(self: *const OpenSslLib, ctx: ?*anyopaque, ciphers: [*:0]const u8) bool {
        return self.SSL_CTX_set_cipher_list(ctx, ciphers) == 1;
    }

    /// Convenience: Get the peer certificate's subject DN as a string.
    /// Returns null if no peer cert is available or if the extraction functions
    /// were not loaded (e.g., older OpenSSL without SSL_get1_peer_certificate).
    /// Caller must call X509_free on the returned cert when done via freePeerCert.
    pub fn getPeerCertSubject(self: *const OpenSslLib, ssl: ?*anyopaque, buf: []u8) ?[]const u8 {
        const get_cert_fn = self.SSL_get1_peer_certificate orelse return null;
        const get_name_fn = self.X509_get_subject_name orelse return null;
        const name_oneline_fn = self.X509_NAME_oneline orelse return null;
        const free_fn = self.X509_free orelse return null;

        const x509 = get_cert_fn(ssl) orelse return null;
        defer free_fn(x509);

        const name = get_name_fn(x509) orelse return null;
        const result = name_oneline_fn(name, buf.ptr, @intCast(buf.len)) orelse return null;
        return std.mem.sliceTo(result, 0);
    }

    /// Enable hostname verification on an SSL connection (client-side).
    /// Must be called BEFORE the TLS handshake (SSL_connect). OpenSSL will then
    /// verify the peer certificate's SAN/CN against the given hostname during
    /// the handshake. Returns true on success.
    pub fn setHostnameVerification(self: *const OpenSslLib, ssl: ?*anyopaque, hostname: [*:0]const u8) bool {
        const set_host_fn = self.SSL_set1_host orelse {
            log.warn("SSL_set1_host not available — hostname verification disabled", .{});
            return false;
        };
        // Use strict wildcard matching (no partial wildcards like f*.example.com)
        if (self.SSL_set_hostflags) |set_flags_fn| {
            set_flags_fn(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
        }
        return set_host_fn(ssl, hostname) == 1;
    }

    /// Check whether the peer certificate chain was verified successfully.
    /// Must be called AFTER the TLS handshake completes. Returns the OpenSSL
    /// verification result code (X509_V_OK = 0 means success).
    pub fn getVerifyResult(self: *const OpenSslLib, ssl: ?*anyopaque) c_long {
        const verify_fn = self.SSL_get_verify_result orelse return -1;
        return verify_fn(ssl);
    }

    /// Check whether the peer certificate has expired or is not yet valid.
    /// Returns .valid, .expired, .not_yet_valid, or .unknown.
    /// Must be called AFTER the TLS handshake completes.
    pub fn checkPeerCertExpiry(self: *const OpenSslLib, ssl: ?*anyopaque) CertExpiryStatus {
        const get_cert_fn = self.SSL_get1_peer_certificate orelse return .unknown;
        const free_fn = self.X509_free orelse return .unknown;
        const get_not_after_fn = self.X509_get0_notAfter orelse return .unknown;
        const get_not_before_fn = self.X509_get0_notBefore orelse return .unknown;
        const cmp_time_fn = self.X509_cmp_current_time orelse return .unknown;

        const x509 = get_cert_fn(ssl) orelse return .no_certificate;
        defer free_fn(x509);

        // Check notBefore: if result > 0, notBefore is in the future → cert not yet valid
        const not_before = get_not_before_fn(x509) orelse return .unknown;
        const before_cmp = cmp_time_fn(not_before);
        if (before_cmp > 0) return .not_yet_valid;

        // Check notAfter: if result < 0, notAfter is in the past → cert expired
        const not_after = get_not_after_fn(x509) orelse return .unknown;
        const after_cmp = cmp_time_fn(not_after);
        if (after_cmp < 0) return .expired;
        if (after_cmp == 0) return .unknown; // 0 means error in X509_cmp_current_time

        return .valid;
    }

    /// Result of certificate expiry check.
    pub const CertExpiryStatus = enum {
        valid,
        expired,
        not_yet_valid,
        no_certificate,
        unknown,

        pub fn isValid(self: CertExpiryStatus) bool {
            return self == .valid;
        }
    };

    /// Extract the mTLS client principal from the peer certificate subject DN.
    /// Returns a Kafka-style principal string "User:CN=..." or the full subject
    /// if CN is not present. The caller owns the returned string.
    /// Returns null if no peer certificate or extraction functions unavailable.
    pub fn extractMtlsPrincipal(self: *const OpenSslLib, ssl: ?*anyopaque, allocator: std.mem.Allocator) ?[]u8 {
        return self.extractMtlsPrincipalWithRules(ssl, allocator, null) catch null;
    }

    /// Extract the mTLS client principal and apply Kafka-style principal mapping
    /// rules when configured. Unsupported rule syntax fails closed so a broker
    /// does not silently authenticate a client as the wrong principal.
    pub fn extractMtlsPrincipalWithRules(
        self: *const OpenSslLib,
        ssl: ?*anyopaque,
        allocator: std.mem.Allocator,
        mapping_rules: ?[]const u8,
    ) !?[]u8 {
        var subject_buf: [1024]u8 = undefined;
        const subject_dn = self.getPeerCertSubject(ssl, &subject_buf) orelse return null;
        return try formatMtlsPrincipalFromSubject(allocator, subject_dn, mapping_rules);
    }

    /// Parse the CN (Common Name) field from an X509 subject DN string.
    /// Input format: "/C=US/ST=State/O=Org/CN=some-name/..."
    /// Returns just the CN value ("some-name") or null if not found.
    pub fn extractCnFromDn(dn: []const u8) ?[]const u8 {
        // Look for "/CN=" prefix (standard OpenSSL oneline format)
        const cn_prefix = "/CN=";
        const cn_start_idx = std.mem.indexOf(u8, dn, cn_prefix) orelse return null;
        const value_start = cn_start_idx + cn_prefix.len;
        if (value_start >= dn.len) return null;

        // CN value ends at the next '/' or end of string
        const remaining = dn[value_start..];
        const end_idx = std.mem.indexOf(u8, remaining, "/") orelse remaining.len;
        if (end_idx == 0) return null;
        return remaining[0..end_idx];
    }

    /// Format an mTLS principal from an X.500 subject DN. With no mapping rules
    /// this preserves the historical ZMQ behavior: extract CN and return
    /// "User:<CN>", falling back to the original subject DN. When rules are
    /// present, this supports Kafka-style RULE:pattern/replacement/[LU] entries
    /// and DEFAULT against a comma-separated DN normalized from OpenSSL oneline.
    pub fn formatMtlsPrincipalFromSubject(
        allocator: std.mem.Allocator,
        subject_dn: []const u8,
        mapping_rules: ?[]const u8,
    ) ![]u8 {
        const rules_text = mapping_rules orelse return formatDefaultMtlsPrincipal(allocator, subject_dn);
        const trimmed_rules = std.mem.trim(u8, rules_text, " \t\r\n");
        if (trimmed_rules.len == 0) return error.InvalidPrincipalMappingRules;

        const normalized_dn = try normalizeSubjectDn(allocator, subject_dn);
        defer allocator.free(normalized_dn);

        var pos: usize = 0;
        var saw_rule = false;
        while (try nextPrincipalRule(trimmed_rules, &pos)) |rule| {
            saw_rule = true;
            switch (rule) {
                .fallback => {
                    return std.fmt.allocPrint(allocator, "User:{s}", .{normalized_dn});
                },
                .rule => |r| {
                    const match = try matchPrincipalPattern(r.pattern, normalized_dn);
                    if (!match.matched) continue;

                    const mapped_name = try applyPrincipalReplacement(
                        allocator,
                        r.replacement,
                        normalized_dn,
                        match,
                    );
                    defer allocator.free(mapped_name);
                    applyPrincipalTransform(mapped_name, r.transform);
                    if (mapped_name.len == 0) return error.EmptyPrincipalMappingResult;
                    return std.fmt.allocPrint(allocator, "User:{s}", .{mapped_name});
                },
            }
        }

        if (!saw_rule) return error.InvalidPrincipalMappingRules;
        return error.PrincipalMappingRuleNoMatch;
    }

    pub fn validatePrincipalMappingRules(rules_text: []const u8) !void {
        const trimmed_rules = std.mem.trim(u8, rules_text, " \t\r\n");
        if (trimmed_rules.len == 0) return error.InvalidPrincipalMappingRules;

        var pos: usize = 0;
        var saw_rule = false;
        while (try nextPrincipalRule(trimmed_rules, &pos)) |_| {
            saw_rule = true;
        }
        if (!saw_rule) return error.InvalidPrincipalMappingRules;
    }

    const PrincipalTransform = enum {
        none,
        lower,
        upper,
    };

    const PrincipalRegexRule = struct {
        pattern: []const u8,
        replacement: []const u8,
        transform: PrincipalTransform,
    };

    const PrincipalRule = union(enum) {
        fallback,
        rule: PrincipalRegexRule,
    };

    const max_principal_captures = 9;

    const PrincipalCaptures = struct {
        count: usize = 0,
        values: [max_principal_captures]?[]const u8 = [_]?[]const u8{null} ** max_principal_captures,
    };

    const PrincipalPatternMatch = struct {
        matched: bool,
        captures: PrincipalCaptures = .{},
    };

    const PrincipalMappingError = error{
        EmptyPrincipalMappingResult,
        InvalidPrincipalMappingRules,
        OutOfMemory,
        PrincipalMappingRuleNoMatch,
        UnsupportedPrincipalMappingRule,
    };

    fn formatDefaultMtlsPrincipal(allocator: std.mem.Allocator, subject_dn: []const u8) ![]u8 {
        if (extractCnFromDn(subject_dn)) |cn| {
            return std.fmt.allocPrint(allocator, "User:{s}", .{cn});
        }
        return std.fmt.allocPrint(allocator, "User:{s}", .{subject_dn});
    }

    fn normalizeSubjectDn(allocator: std.mem.Allocator, subject_dn: []const u8) ![]u8 {
        if (!std.mem.startsWith(u8, subject_dn, "/")) {
            return try allocator.dupe(u8, subject_dn);
        }

        var normalized = std.array_list.Managed(u8).init(allocator);
        errdefer normalized.deinit();

        var first = true;
        var parts = std.mem.splitScalar(u8, subject_dn, '/');
        while (parts.next()) |part| {
            if (part.len == 0) continue;
            if (!first) try normalized.append(',');
            try normalized.appendSlice(part);
            first = false;
        }
        return try normalized.toOwnedSlice();
    }

    fn nextPrincipalRule(rules_text: []const u8, pos: *usize) !?PrincipalRule {
        skipSpaces(rules_text, pos);
        if (pos.* >= rules_text.len) return null;
        if (rules_text[pos.*] == ',') return error.InvalidPrincipalMappingRules;

        if (startsWithAt(rules_text, pos.*, "DEFAULT")) {
            var end = pos.* + "DEFAULT".len;
            skipSpacesAt(rules_text, &end);
            if (end < rules_text.len and rules_text[end] != ',') {
                return error.InvalidPrincipalMappingRules;
            }
            pos.* = try consumeRuleSeparator(rules_text, end);
            return PrincipalRule{ .fallback = {} };
        }

        if (!startsWithAt(rules_text, pos.*, "RULE:")) {
            return error.InvalidPrincipalMappingRules;
        }

        var cursor = pos.* + "RULE:".len;
        const pattern_start = cursor;
        const first_delim = try findRuleDelimiter(rules_text, cursor);
        const pattern = rules_text[pattern_start..first_delim];
        cursor = first_delim + 1;

        const replacement_start = cursor;
        const second_delim = try findRuleDelimiter(rules_text, cursor);
        const replacement = rules_text[replacement_start..second_delim];
        cursor = second_delim + 1;

        var transform: PrincipalTransform = .none;
        if (cursor < rules_text.len) {
            switch (rules_text[cursor]) {
                'L' => {
                    transform = .lower;
                    cursor += 1;
                },
                'U' => {
                    transform = .upper;
                    cursor += 1;
                },
                else => {},
            }
        }

        skipSpacesAt(rules_text, &cursor);
        if (cursor < rules_text.len and rules_text[cursor] != ',') {
            return error.InvalidPrincipalMappingRules;
        }

        try validatePrincipalPattern(pattern);
        try validatePrincipalReplacement(replacement);

        pos.* = try consumeRuleSeparator(rules_text, cursor);
        return PrincipalRule{ .rule = .{
            .pattern = pattern,
            .replacement = replacement,
            .transform = transform,
        } };
    }

    fn skipSpaces(text: []const u8, pos: *usize) void {
        skipSpacesAt(text, pos);
    }

    fn skipSpacesAt(text: []const u8, pos: *usize) void {
        while (pos.* < text.len) {
            switch (text[pos.*]) {
                ' ', '\t', '\r', '\n' => pos.* += 1,
                else => return,
            }
        }
    }

    fn startsWithAt(text: []const u8, index: usize, prefix: []const u8) bool {
        return index <= text.len and std.mem.startsWith(u8, text[index..], prefix);
    }

    fn consumeRuleSeparator(text: []const u8, index: usize) !usize {
        var cursor = index;
        skipSpacesAt(text, &cursor);
        if (cursor >= text.len) return cursor;
        if (text[cursor] != ',') return error.InvalidPrincipalMappingRules;
        cursor += 1;

        var next = cursor;
        skipSpacesAt(text, &next);
        if (next >= text.len) return error.InvalidPrincipalMappingRules;
        return cursor;
    }

    fn findRuleDelimiter(text: []const u8, start: usize) !usize {
        var i = start;
        while (i < text.len) {
            if (text[i] == '\\') {
                if (i + 1 >= text.len) return error.InvalidPrincipalMappingRules;
                i += 2;
                continue;
            }
            if (text[i] == '/') return i;
            i += 1;
        }
        return error.InvalidPrincipalMappingRules;
    }

    fn validatePrincipalPattern(pattern: []const u8) !void {
        if (pattern.len == 0) return error.InvalidPrincipalMappingRules;

        var i: usize = 0;
        var captures: usize = 0;
        while (i < pattern.len) {
            switch (pattern[i]) {
                '\\' => {
                    if (i + 1 >= pattern.len) return error.InvalidPrincipalMappingRules;
                    i += 2;
                },
                '^' => {
                    if (i != 0) return error.UnsupportedPrincipalMappingRule;
                    i += 1;
                },
                '$' => {
                    if (i + 1 != pattern.len) return error.UnsupportedPrincipalMappingRule;
                    i += 1;
                },
                '.' => {
                    const wildcard_len = try wildcardTokenLen(pattern, i);
                    i += wildcard_len;
                },
                '(' => {
                    if (captures >= max_principal_captures) return error.UnsupportedPrincipalMappingRule;
                    const end = findCaptureEnd(pattern, i + 1) orelse return error.InvalidPrincipalMappingRules;
                    try validateCapturePattern(pattern[i + 1 .. end]);
                    captures += 1;
                    i = end + 1;
                },
                ')', '[', ']', '*', '+', '?', '|' => return error.UnsupportedPrincipalMappingRule,
                else => i += 1,
            }
        }
    }

    fn validateCapturePattern(inner: []const u8) !void {
        _ = try parseCapturePattern(inner);
    }

    fn validatePrincipalReplacement(replacement: []const u8) !void {
        if (replacement.len == 0) return error.InvalidPrincipalMappingRules;

        var i: usize = 0;
        while (i < replacement.len) {
            switch (replacement[i]) {
                '\\' => {
                    if (i + 1 >= replacement.len) return error.InvalidPrincipalMappingRules;
                    i += 2;
                },
                '$' => {
                    if (i + 1 >= replacement.len) return error.InvalidPrincipalMappingRules;
                    if (replacement[i + 1] >= '0' and replacement[i + 1] <= '9') {
                        i += 2;
                    } else if (replacement[i + 1] == '{') {
                        if (i + 3 >= replacement.len or replacement[i + 2] < '0' or replacement[i + 2] > '9' or replacement[i + 3] != '}') {
                            return error.UnsupportedPrincipalMappingRule;
                        }
                        i += 4;
                    } else {
                        return error.UnsupportedPrincipalMappingRule;
                    }
                },
                else => i += 1,
            }
        }
    }

    fn wildcardTokenLen(pattern: []const u8, index: usize) !usize {
        if (index + 1 >= pattern.len) return error.UnsupportedPrincipalMappingRule;
        if (pattern[index] != '.') return error.UnsupportedPrincipalMappingRule;
        if (pattern[index + 1] != '*' and pattern[index + 1] != '+') {
            return error.UnsupportedPrincipalMappingRule;
        }
        if (index + 2 < pattern.len and pattern[index + 2] == '?') return 3;
        return 2;
    }

    fn findCaptureEnd(pattern: []const u8, start: usize) ?usize {
        var i = start;
        while (i < pattern.len) : (i += 1) {
            if (pattern[i] == '\\') {
                i += 1;
                continue;
            }
            if (pattern[i] == ')') return i;
            if (pattern[i] == '(') return null;
        }
        return null;
    }

    const CaptureKind = enum {
        any,
        char_class,
        not_char,
    };

    const CapturePattern = struct {
        kind: CaptureKind,
        class: []const u8 = "",
        excluded: u8 = 0,
        min_len: usize,
        greedy: bool,
    };

    fn parseCapturePattern(inner: []const u8) !CapturePattern {
        if (inner.len == 2 or inner.len == 3) {
            if (inner[0] == '.' and (inner[1] == '*' or inner[1] == '+')) {
                if (inner.len == 3 and inner[2] != '?') return error.UnsupportedPrincipalMappingRule;
                return .{
                    .kind = .any,
                    .min_len = if (inner[1] == '+') 1 else 0,
                    .greedy = inner.len != 3,
                };
            }
        }

        if (inner.len >= 4 and inner[0] == '[') {
            const class_end = findCharacterClassEnd(inner, 1) orelse return error.InvalidPrincipalMappingRules;
            if (class_end + 1 >= inner.len) return error.UnsupportedPrincipalMappingRule;
            const quantifier = inner[class_end + 1];
            if (quantifier != '*' and quantifier != '+') return error.UnsupportedPrincipalMappingRule;
            if (class_end + 2 < inner.len and inner[class_end + 2] != '?') return error.UnsupportedPrincipalMappingRule;
            if (class_end + 3 < inner.len) return error.UnsupportedPrincipalMappingRule;

            const class = inner[1..class_end];
            if (class.len == 0) return error.UnsupportedPrincipalMappingRule;
            if (class.len == 2 and class[0] == '^') {
                return .{
                    .kind = .not_char,
                    .excluded = class[1],
                    .min_len = if (quantifier == '+') 1 else 0,
                    .greedy = class_end + 2 >= inner.len,
                };
            }
            if (class[0] == '^') return error.UnsupportedPrincipalMappingRule;
            try validateCharacterClass(class);
            return .{
                .kind = .char_class,
                .class = class,
                .min_len = if (quantifier == '+') 1 else 0,
                .greedy = class_end + 2 >= inner.len,
            };
        }

        return error.UnsupportedPrincipalMappingRule;
    }

    fn findCharacterClassEnd(pattern: []const u8, start: usize) ?usize {
        var i = start;
        while (i < pattern.len) : (i += 1) {
            if (pattern[i] == '\\') {
                i += 1;
                continue;
            }
            if (pattern[i] == ']') return i;
        }
        return null;
    }

    fn validateCharacterClass(class: []const u8) !void {
        var i: usize = 0;
        while (i < class.len) {
            if (class[i] == '\\') {
                if (i + 1 >= class.len) return error.InvalidPrincipalMappingRules;
                i += 2;
                continue;
            }
            if (i + 2 < class.len and class[i + 1] == '-') {
                if (class[i] > class[i + 2]) return error.UnsupportedPrincipalMappingRule;
                i += 3;
                continue;
            }
            i += 1;
        }
    }

    fn matchPrincipalPattern(pattern: []const u8, input: []const u8) PrincipalMappingError!PrincipalPatternMatch {
        var captures = PrincipalCaptures{};
        const matched = try matchPrincipalPatternFrom(pattern, 0, input, 0, &captures);
        return .{ .matched = matched, .captures = captures };
    }

    fn matchPrincipalPatternFrom(
        pattern: []const u8,
        pattern_index: usize,
        input: []const u8,
        input_index: usize,
        captures: *PrincipalCaptures,
    ) PrincipalMappingError!bool {
        if (pattern_index == pattern.len) return input_index == input.len;

        const c = pattern[pattern_index];
        switch (c) {
            '^' => {
                if (input_index != 0) return false;
                return try matchPrincipalPatternFrom(pattern, pattern_index + 1, input, input_index, captures);
            },
            '$' => return input_index == input.len and pattern_index + 1 == pattern.len,
            '\\' => {
                if (pattern_index + 1 >= pattern.len) return error.InvalidPrincipalMappingRules;
                return try matchLiteralAndContinue(pattern, pattern_index + 2, input, input_index, pattern[pattern_index + 1], captures);
            },
            '.' => {
                const token_len = try wildcardTokenLen(pattern, pattern_index);
                const min_len: usize = if (pattern[pattern_index + 1] == '+') 1 else 0;
                const greedy = !(token_len == 3 and pattern[pattern_index + 2] == '?');
                return try matchWildcardAndContinue(
                    pattern,
                    pattern_index + token_len,
                    input,
                    input_index,
                    min_len,
                    greedy,
                    captures,
                );
            },
            '(' => {
                if (captures.count >= max_principal_captures) return error.UnsupportedPrincipalMappingRule;
                const capture_index = captures.count;
                const capture_end = findCaptureEnd(pattern, pattern_index + 1) orelse return error.InvalidPrincipalMappingRules;
                const capture_pattern = try parseCapturePattern(pattern[pattern_index + 1 .. capture_end]);
                return try matchCaptureAndContinue(
                    pattern,
                    capture_end + 1,
                    input,
                    input_index,
                    capture_pattern,
                    capture_index,
                    captures,
                );
            },
            else => return try matchLiteralAndContinue(pattern, pattern_index + 1, input, input_index, c, captures),
        }
    }

    fn matchLiteralAndContinue(
        pattern: []const u8,
        next_pattern_index: usize,
        input: []const u8,
        input_index: usize,
        literal: u8,
        captures: *PrincipalCaptures,
    ) PrincipalMappingError!bool {
        if (input_index >= input.len or input[input_index] != literal) return false;
        return try matchPrincipalPatternFrom(pattern, next_pattern_index, input, input_index + 1, captures);
    }

    fn matchWildcardAndContinue(
        pattern: []const u8,
        next_pattern_index: usize,
        input: []const u8,
        input_index: usize,
        min_len: usize,
        greedy: bool,
        captures: *PrincipalCaptures,
    ) PrincipalMappingError!bool {
        if (input_index + min_len > input.len) return false;

        if (greedy) {
            var end = input.len;
            while (end >= input_index + min_len) {
                var branch_captures = captures.*;
                if (try matchPrincipalPatternFrom(pattern, next_pattern_index, input, end, &branch_captures)) {
                    captures.* = branch_captures;
                    return true;
                }
                if (end == input_index + min_len) break;
                end -= 1;
            }
            return false;
        }

        var end = input_index + min_len;
        while (end <= input.len) : (end += 1) {
            var branch_captures = captures.*;
            if (try matchPrincipalPatternFrom(pattern, next_pattern_index, input, end, &branch_captures)) {
                captures.* = branch_captures;
                return true;
            }
        }
        return false;
    }

    fn matchCaptureAndContinue(
        pattern: []const u8,
        next_pattern_index: usize,
        input: []const u8,
        input_index: usize,
        capture_pattern: CapturePattern,
        capture_index: usize,
        captures: *PrincipalCaptures,
    ) PrincipalMappingError!bool {
        const max_end = captureMaxEnd(input, input_index, capture_pattern);
        if (input_index + capture_pattern.min_len > max_end) return false;

        if (capture_pattern.greedy) {
            var end = max_end;
            while (end >= input_index + capture_pattern.min_len) {
                var branch_captures = captures.*;
                branch_captures.values[capture_index] = input[input_index..end];
                branch_captures.count = @max(branch_captures.count, capture_index + 1);
                if (try matchPrincipalPatternFrom(pattern, next_pattern_index, input, end, &branch_captures)) {
                    captures.* = branch_captures;
                    return true;
                }
                if (end == input_index + capture_pattern.min_len) break;
                end -= 1;
            }
            return false;
        }

        var end = input_index + capture_pattern.min_len;
        while (end <= max_end) : (end += 1) {
            var branch_captures = captures.*;
            branch_captures.values[capture_index] = input[input_index..end];
            branch_captures.count = @max(branch_captures.count, capture_index + 1);
            if (try matchPrincipalPatternFrom(pattern, next_pattern_index, input, end, &branch_captures)) {
                captures.* = branch_captures;
                return true;
            }
        }
        return false;
    }

    fn captureMaxEnd(input: []const u8, input_index: usize, capture_pattern: CapturePattern) usize {
        switch (capture_pattern.kind) {
            .any => return input.len,
            .char_class => {
                var end = input_index;
                while (end < input.len and characterClassContains(capture_pattern.class, input[end])) : (end += 1) {}
                return end;
            },
            .not_char => {
                var end = input_index;
                while (end < input.len and input[end] != capture_pattern.excluded) : (end += 1) {}
                return end;
            },
        }
    }

    fn characterClassContains(class: []const u8, value: u8) bool {
        var i: usize = 0;
        while (i < class.len) {
            var first = class[i];
            if (first == '\\') {
                if (i + 1 >= class.len) return false;
                first = class[i + 1];
                if (first == value) return true;
                i += 2;
                continue;
            }
            if (i + 2 < class.len and class[i + 1] == '-') {
                const last = class[i + 2];
                if (first <= value and value <= last) return true;
                i += 3;
                continue;
            }
            if (first == value) return true;
            i += 1;
        }
        return false;
    }

    fn applyPrincipalReplacement(
        allocator: std.mem.Allocator,
        replacement: []const u8,
        input: []const u8,
        match: PrincipalPatternMatch,
    ) ![]u8 {
        var output = std.array_list.Managed(u8).init(allocator);
        errdefer output.deinit();

        var i: usize = 0;
        while (i < replacement.len) {
            switch (replacement[i]) {
                '\\' => {
                    if (i + 1 >= replacement.len) return error.InvalidPrincipalMappingRules;
                    try output.append(replacement[i + 1]);
                    i += 2;
                },
                '$' => {
                    if (i + 1 >= replacement.len) return error.InvalidPrincipalMappingRules;
                    if (replacement[i + 1] == '0') {
                        try output.appendSlice(input);
                        i += 2;
                    } else if (replacement[i + 1] >= '1' and replacement[i + 1] <= '9') {
                        const capture_index: usize = @intCast(replacement[i + 1] - '1');
                        try output.appendSlice(try capturedPrincipalGroup(match, capture_index));
                        i += 2;
                    } else if (replacement[i + 1] == '{') {
                        if (i + 3 >= replacement.len or replacement[i + 3] != '}') {
                            return error.InvalidPrincipalMappingRules;
                        }
                        switch (replacement[i + 2]) {
                            '0' => try output.appendSlice(input),
                            '1'...'9' => |capture_ref| {
                                const capture_index: usize = @intCast(capture_ref - '1');
                                try output.appendSlice(try capturedPrincipalGroup(match, capture_index));
                            },
                            else => return error.UnsupportedPrincipalMappingRule,
                        }
                        i += 4;
                    } else {
                        return error.UnsupportedPrincipalMappingRule;
                    }
                },
                else => {
                    try output.append(replacement[i]);
                    i += 1;
                },
            }
        }

        return try output.toOwnedSlice();
    }

    fn capturedPrincipalGroup(match: PrincipalPatternMatch, capture_index: usize) PrincipalMappingError![]const u8 {
        if (capture_index >= match.captures.count) return error.UnsupportedPrincipalMappingRule;
        return match.captures.values[capture_index] orelse return error.UnsupportedPrincipalMappingRule;
    }

    fn applyPrincipalTransform(text: []u8, transform: PrincipalTransform) void {
        switch (transform) {
            .none => {},
            .lower => {
                for (text) |*c| c.* = asciiLower(c.*);
            },
            .upper => {
                for (text) |*c| c.* = asciiUpper(c.*);
            },
        }
    }

    fn asciiLower(c: u8) u8 {
        if (c >= 'A' and c <= 'Z') return c + ('a' - 'A');
        return c;
    }

    fn asciiUpper(c: u8) u8 {
        if (c >= 'a' and c <= 'z') return c - ('a' - 'A');
        return c;
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
        return @ptrCast(@alignCast(ptr));
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
            .TLS_client_method = undefined,
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
            .SSL_connect = undefined,
            .SSL_read = undefined,
            .SSL_write = undefined,
            .SSL_shutdown = undefined,
            .SSL_get_error = undefined,
            .SSL_CTX_set_cipher_list = undefined,
            .OPENSSL_init_ssl = undefined,
            .ERR_get_error = undefined,
            .ERR_error_string_n = undefined,
        };

        // Load libssl functions
        self.TLS_server_method = try lookupFn(ssl_handle, @TypeOf(self.TLS_server_method), "TLS_server_method");
        self.TLS_client_method = try lookupFn(ssl_handle, @TypeOf(self.TLS_client_method), "TLS_client_method");
        self.SSL_CTX_new = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_new), "SSL_CTX_new");
        self.SSL_CTX_free = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_free), "SSL_CTX_free");
        self.SSL_CTX_use_certificate_chain_file = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_use_certificate_chain_file), "SSL_CTX_use_certificate_chain_file");
        self.SSL_CTX_use_PrivateKey_file = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_use_PrivateKey_file), "SSL_CTX_use_PrivateKey_file");
        self.SSL_CTX_check_private_key = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_check_private_key), "SSL_CTX_check_private_key");
        self.SSL_CTX_load_verify_locations = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_load_verify_locations), "SSL_CTX_load_verify_locations");
        self.SSL_CTX_set_verify = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_set_verify), "SSL_CTX_set_verify");
        self.SSL_CTX_set_default_verify_paths = lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_set_default_verify_paths.?), "SSL_CTX_set_default_verify_paths") catch null;
        self.SSL_CTX_ctrl = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_ctrl), "SSL_CTX_ctrl");
        self.SSL_new = try lookupFn(ssl_handle, @TypeOf(self.SSL_new), "SSL_new");
        self.SSL_free = try lookupFn(ssl_handle, @TypeOf(self.SSL_free), "SSL_free");
        self.SSL_set_fd = try lookupFn(ssl_handle, @TypeOf(self.SSL_set_fd), "SSL_set_fd");
        self.SSL_accept = try lookupFn(ssl_handle, @TypeOf(self.SSL_accept), "SSL_accept");
        self.SSL_connect = try lookupFn(ssl_handle, @TypeOf(self.SSL_connect), "SSL_connect");
        self.SSL_read = try lookupFn(ssl_handle, @TypeOf(self.SSL_read), "SSL_read");
        self.SSL_write = try lookupFn(ssl_handle, @TypeOf(self.SSL_write), "SSL_write");
        self.SSL_shutdown = try lookupFn(ssl_handle, @TypeOf(self.SSL_shutdown), "SSL_shutdown");
        self.SSL_get_error = try lookupFn(ssl_handle, @TypeOf(self.SSL_get_error), "SSL_get_error");
        self.SSL_CTX_set_cipher_list = try lookupFn(ssl_handle, @TypeOf(self.SSL_CTX_set_cipher_list), "SSL_CTX_set_cipher_list");

        // Peer certificate functions (optional — soft-fail if not found).
        // OpenSSL 3.x renamed SSL_get_peer_certificate → SSL_get1_peer_certificate.
        // Try the new name first, then fall back to the old name.
        self.SSL_get1_peer_certificate = lookupFn(ssl_handle, @TypeOf(self.SSL_get1_peer_certificate.?), "SSL_get1_peer_certificate") catch
            lookupFn(ssl_handle, @TypeOf(self.SSL_get1_peer_certificate.?), "SSL_get_peer_certificate") catch null;
        self.X509_get_subject_name = lookupFn(crypto_handle, @TypeOf(self.X509_get_subject_name.?), "X509_get_subject_name") catch null;
        self.X509_NAME_oneline = lookupFn(crypto_handle, @TypeOf(self.X509_NAME_oneline.?), "X509_NAME_oneline") catch null;
        self.X509_free = lookupFn(crypto_handle, @TypeOf(self.X509_free.?), "X509_free") catch null;

        // Hostname verification and certificate validation (optional — soft-fail).
        // SSL_set1_host is available in OpenSSL 1.1.0+. It configures the SSL
        // object to verify the peer certificate's SAN/CN against the hostname
        // during the handshake.
        self.SSL_set1_host = lookupFn(ssl_handle, @TypeOf(self.SSL_set1_host.?), "SSL_set1_host") catch null;
        self.SSL_set_hostflags = lookupFn(ssl_handle, @TypeOf(self.SSL_set_hostflags.?), "SSL_set_hostflags") catch null;
        self.SSL_get_verify_result = lookupFn(ssl_handle, @TypeOf(self.SSL_get_verify_result.?), "SSL_get_verify_result") catch null;

        // Certificate time inspection (from libcrypto)
        self.X509_get0_notAfter = lookupFn(crypto_handle, @TypeOf(self.X509_get0_notAfter.?), "X509_get0_notAfter") catch null;
        self.X509_get0_notBefore = lookupFn(crypto_handle, @TypeOf(self.X509_get0_notBefore.?), "X509_get0_notBefore") catch null;
        self.X509_cmp_current_time = lookupFn(crypto_handle, @TypeOf(self.X509_cmp_current_time.?), "X509_cmp_current_time") catch null;
        self.X509_get_issuer_name = lookupFn(crypto_handle, @TypeOf(self.X509_get_issuer_name.?), "X509_get_issuer_name") catch null;
        self.X509_get_serialNumber = lookupFn(crypto_handle, @TypeOf(self.X509_get_serialNumber.?), "X509_get_serialNumber") catch null;

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

test "OpenSslLib hostname verification functions loaded" {
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    // SSL_set1_host should be available on OpenSSL 1.1.0+
    try testing.expect(lib.SSL_set1_host != null);
    try testing.expect(lib.SSL_get_verify_result != null);
}

test "OpenSslLib certificate time functions loaded" {
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    // X509 time functions should be available on any modern OpenSSL
    try testing.expect(lib.X509_get0_notAfter != null);
    try testing.expect(lib.X509_get0_notBefore != null);
    try testing.expect(lib.X509_cmp_current_time != null);
}

test "extractCnFromDn parses standard DN" {
    // Standard OpenSSL oneline format
    const dn = "/C=US/ST=California/O=ZMQ/CN=kafka-client-1";
    const cn = OpenSslLib.extractCnFromDn(dn);
    try testing.expect(cn != null);
    try testing.expectEqualStrings("kafka-client-1", cn.?);
}

test "extractCnFromDn handles CN in middle" {
    const dn = "/C=US/CN=broker-node/O=ZMQ";
    const cn = OpenSslLib.extractCnFromDn(dn);
    try testing.expect(cn != null);
    try testing.expectEqualStrings("broker-node", cn.?);
}

test "extractCnFromDn returns null for missing CN" {
    const dn = "/C=US/ST=California/O=ZMQ";
    const cn = OpenSslLib.extractCnFromDn(dn);
    try testing.expect(cn == null);
}

test "extractCnFromDn handles empty CN value" {
    const dn = "/C=US/CN=/O=ZMQ";
    const cn = OpenSslLib.extractCnFromDn(dn);
    try testing.expect(cn == null);
}

test "extractCnFromDn handles CN at end without trailing slash" {
    const dn = "/O=ZMQ/CN=my-service";
    const cn = OpenSslLib.extractCnFromDn(dn);
    try testing.expect(cn != null);
    try testing.expectEqualStrings("my-service", cn.?);
}

test "formatMtlsPrincipalFromSubject preserves default CN extraction" {
    const principal = try OpenSslLib.formatMtlsPrincipalFromSubject(
        testing.allocator,
        "/C=US/ST=California/O=ZMQ/CN=kafka-client-1",
        null,
    );
    defer testing.allocator.free(principal);

    try testing.expectEqualStrings("User:kafka-client-1", principal);
}

test "formatMtlsPrincipalFromSubject applies Kafka-style rule with lower transform" {
    const principal = try OpenSslLib.formatMtlsPrincipalFromSubject(
        testing.allocator,
        "/C=US/ST=California/O=ZMQ/CN=Kafka-Client-1",
        "RULE:.*CN=([^,]+).*/$1/L,DEFAULT",
    );
    defer testing.allocator.free(principal);

    try testing.expectEqualStrings("User:kafka-client-1", principal);
}

test "formatMtlsPrincipalFromSubject applies multi-capture character-class rule" {
    const principal = try OpenSslLib.formatMtlsPrincipalFromSubject(
        testing.allocator,
        "/C=US/O=ZMQ/OU=ServiceUsers/CN=Kafka_Client-1",
        "RULE:.*OU=([A-Za-z]+),CN=([A-Za-z0-9._-]+).*/$2@$1/L,DEFAULT",
    );
    defer testing.allocator.free(principal);

    try testing.expectEqualStrings("User:kafka_client-1@serviceusers", principal);
}

test "formatMtlsPrincipalFromSubject falls back to normalized DN on DEFAULT" {
    const principal = try OpenSslLib.formatMtlsPrincipalFromSubject(
        testing.allocator,
        "/C=US/O=ZMQ/CN=kafka-client-1",
        "RULE:^OU=([^,]+)$/$1/,DEFAULT",
    );
    defer testing.allocator.free(principal);

    try testing.expectEqualStrings("User:C=US,O=ZMQ,CN=kafka-client-1", principal);
}

test "validatePrincipalMappingRules rejects unsupported regex syntax" {
    try testing.expectError(
        error.UnsupportedPrincipalMappingRule,
        OpenSslLib.validatePrincipalMappingRules("RULE:.*CN=(foo|bar).*/$1/"),
    );
}

test "formatMtlsPrincipalFromSubject rejects replacement for missing capture" {
    try testing.expectError(
        error.UnsupportedPrincipalMappingRule,
        OpenSslLib.formatMtlsPrincipalFromSubject(
            testing.allocator,
            "/C=US/O=ZMQ/CN=kafka-client-1",
            "RULE:.*CN=([^,]+).*/$2/",
        ),
    );
}

test "CertExpiryStatus isValid" {
    try testing.expect(OpenSslLib.CertExpiryStatus.valid.isValid());
    try testing.expect(!OpenSslLib.CertExpiryStatus.expired.isValid());
    try testing.expect(!OpenSslLib.CertExpiryStatus.not_yet_valid.isValid());
    try testing.expect(!OpenSslLib.CertExpiryStatus.no_certificate.isValid());
    try testing.expect(!OpenSslLib.CertExpiryStatus.unknown.isValid());
}

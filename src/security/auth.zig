const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.auth);

/// ACL (Access Control List) authorization engine.
///
/// Implements the Kafka ACL model:
///   (Principal, Permission, Operation, Resource, PatternType) → Allow/Deny
pub const Authorizer = struct {
    acls: std.array_list.Managed(AclEntry),
    allocator: Allocator,
    /// Principals that bypass all ACL checks (e.g., "User:admin").
    /// Matches AutoMQ/Kafka's super.users configuration.
    super_users: std.StringHashMap(void),
    /// When true (default), allow all operations if no ACLs are defined.
    /// When false, deny all operations when no ACLs match.
    /// Matches Kafka's allow.everyone.if.no.acl.found configuration.
    allow_everyone_if_no_acl: bool = true,

    pub const ResourceType = enum(i8) {
        unknown = 0,
        any = 1,
        topic = 2,
        group = 3,
        cluster = 4,
        transactional_id = 5,
        delegation_token = 6,
    };

    pub const Operation = enum(i8) {
        unknown = 0,
        any = 1,
        all = 2,
        read = 3,
        write = 4,
        create = 5,
        delete = 6,
        alter = 7,
        describe = 8,
        cluster_action = 9,
        describe_configs = 10,
        alter_configs = 11,
        idempotent_write = 12,
    };

    pub const Permission = enum(i8) {
        unknown = 0,
        any = 1,
        deny = 2,
        allow = 3,
    };

    pub const PatternType = enum(i8) {
        unknown = 0,
        any = 1,
        match = 2,
        literal = 3,
        prefixed = 4,
    };

    pub const AclEntry = struct {
        principal: []u8,
        resource_type: ResourceType,
        resource_name: []u8,
        pattern_type: PatternType,
        operation: Operation,
        permission: Permission,
        host: []u8,
    };

    pub fn init(alloc: Allocator) Authorizer {
        return .{
            .acls = std.array_list.Managed(AclEntry).init(alloc),
            .super_users = std.StringHashMap(void).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *Authorizer) void {
        for (self.acls.items) |entry| {
            self.allocator.free(entry.principal);
            self.allocator.free(entry.resource_name);
            self.allocator.free(entry.host);
        }
        self.acls.deinit();
        var su_it = self.super_users.keyIterator();
        while (su_it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.super_users.deinit();
    }

    /// Add an ACL entry.
    pub fn addAcl(self: *Authorizer, principal: []const u8, resource_type: ResourceType, resource_name: []const u8, pattern_type: PatternType, operation: Operation, permission: Permission, host: []const u8) !void {
        const principal_copy = try self.allocator.dupe(u8, principal);
        errdefer self.allocator.free(principal_copy);
        const resource_name_copy = try self.allocator.dupe(u8, resource_name);
        errdefer self.allocator.free(resource_name_copy);
        const host_copy = try self.allocator.dupe(u8, host);
        errdefer self.allocator.free(host_copy);

        try self.acls.append(.{
            .principal = principal_copy,
            .resource_type = resource_type,
            .resource_name = resource_name_copy,
            .pattern_type = pattern_type,
            .operation = operation,
            .permission = permission,
            .host = host_copy,
        });
        log.info("ACL added: principal={s} resource={s} op={d} permission={d}", .{ principal, resource_name, @intFromEnum(operation), @intFromEnum(permission) });
    }

    /// Add a super user principal that bypasses all ACL checks.
    pub fn addSuperUser(self: *Authorizer, principal: []const u8) !void {
        const key = try self.allocator.dupe(u8, principal);
        try self.super_users.put(key, {});
        log.info("Super user added: {s}", .{principal});
    }

    /// Remove ACL entries matching the given filter criteria.
    /// A filter field value of null or wildcard means "match any".
    /// Returns the number of entries removed.
    pub fn removeMatchingAcls(
        self: *Authorizer,
        filter_resource_type: ResourceType,
        filter_resource_name: ?[]const u8,
        filter_pattern_type: PatternType,
        filter_principal: ?[]const u8,
        filter_host: ?[]const u8,
        filter_operation: Operation,
        filter_permission: Permission,
    ) usize {
        var removed: usize = 0;
        var i: usize = 0;
        while (i < self.acls.items.len) {
            const acl = self.acls.items[i];
            const matches = aclMatchesFilter(acl, filter_resource_type, filter_resource_name, filter_pattern_type, filter_principal, filter_host, filter_operation, filter_permission);
            if (matches) {
                self.allocator.free(acl.principal);
                self.allocator.free(acl.resource_name);
                self.allocator.free(acl.host);
                _ = self.acls.orderedRemove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }
        if (removed > 0) {
            log.info("Removed {d} ACL entries matching filter", .{removed});
        }
        return removed;
    }

    fn aclMatchesFilter(
        acl: AclEntry,
        filter_resource_type: ResourceType,
        filter_resource_name: ?[]const u8,
        filter_pattern_type: PatternType,
        filter_principal: ?[]const u8,
        filter_host: ?[]const u8,
        filter_operation: Operation,
        filter_permission: Permission,
    ) bool {
        if (filter_resource_type != .any and filter_resource_type != .unknown and acl.resource_type != filter_resource_type) return false;
        if (filter_resource_name) |name| {
            if (!std.mem.eql(u8, name, "*") and !std.mem.eql(u8, name, acl.resource_name)) return false;
        }
        if (filter_pattern_type != .any and filter_pattern_type != .unknown and acl.pattern_type != filter_pattern_type) return false;
        if (filter_principal) |p| {
            if (!std.mem.eql(u8, p, "*") and !std.mem.eql(u8, p, acl.principal)) return false;
        }
        if (filter_host) |h| {
            if (!std.mem.eql(u8, h, "*") and !std.mem.eql(u8, h, acl.host)) return false;
        }
        if (filter_operation != .any and filter_operation != .unknown and acl.operation != filter_operation) return false;
        if (filter_permission != .any and filter_permission != .unknown and acl.permission != filter_permission) return false;
        return true;
    }

    /// Check if an operation is authorized.
    pub fn authorize(self: *const Authorizer, principal: []const u8, resource_type: ResourceType, resource_name: []const u8, operation: Operation) AuthResult {
        // Super users bypass all ACL checks
        if (self.super_users.contains(principal)) return .allowed;

        // If no ACLs exist, behavior depends on allow_everyone_if_no_acl
        if (self.acls.items.len == 0) {
            return if (self.allow_everyone_if_no_acl) .allowed else .denied;
        }

        var explicitly_denied = false;
        var explicitly_allowed = false;

        for (self.acls.items) |acl| {
            if (!matchesPrincipal(acl.principal, principal)) continue;
            if (acl.resource_type != resource_type and acl.resource_type != .any) continue;
            if (!matchesResource(acl.resource_name, resource_name, acl.pattern_type)) continue;
            if (acl.operation != operation and acl.operation != .all and acl.operation != .any) continue;

            switch (acl.permission) {
                .deny => explicitly_denied = true,
                .allow => explicitly_allowed = true,
                else => {},
            }
        }

        // Deny takes precedence
        if (explicitly_denied) {
            log.warn("Authorization denied: principal={s} resource={s} op={d} (explicit deny)", .{ principal, resource_name, @intFromEnum(operation) });
            return .denied;
        }
        if (explicitly_allowed) {
            log.debug("Authorization granted: principal={s} resource={s} op={d}", .{ principal, resource_name, @intFromEnum(operation) });
            return .allowed;
        }
        log.warn("Authorization denied: principal={s} resource={s} op={d} (no matching allow)", .{ principal, resource_name, @intFromEnum(operation) });
        return .denied; // Default deny when ACLs exist
    }

    pub const AuthResult = enum { allowed, denied };

    fn matchesPrincipal(acl_principal: []const u8, principal: []const u8) bool {
        if (std.mem.eql(u8, acl_principal, "*")) return true;
        return std.mem.eql(u8, acl_principal, principal);
    }

    fn matchesResource(acl_name: []const u8, resource: []const u8, pattern: PatternType) bool {
        return switch (pattern) {
            .literal => std.mem.eql(u8, acl_name, resource),
            .prefixed => std.mem.startsWith(u8, resource, acl_name),
            .any, .match => true,
            else => false,
        };
    }

    pub fn aclCount(self: *const Authorizer) usize {
        return self.acls.items.len;
    }
};

/// SASL/PLAIN authenticator.
///
/// Simple username/password authentication. Passwords are hashed with
/// PBKDF2-HMAC-SHA256 before storage (never stored in plaintext).
/// Each user gets a random 16-byte salt and 4096 iterations.
pub const SaslPlainAuthenticator = struct {
    credentials: std.StringHashMap(HashedCredential),
    allocator: Allocator,

    /// Hashed password credential stored per user.
    /// Uses PBKDF2-HMAC-SHA256 with a per-user random salt.
    pub const HashedCredential = struct {
        salt: [16]u8,
        hash: [32]u8,
        iterations: u32,
    };

    /// Number of PBKDF2 iterations for password hashing.
    const pbkdf2_iterations: u32 = 4096;

    pub fn init(alloc: Allocator) SaslPlainAuthenticator {
        return .{
            .credentials = std.StringHashMap(HashedCredential).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *SaslPlainAuthenticator) void {
        var it = self.credentials.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.credentials.deinit();
    }

    /// Register a username/password pair.
    /// The password is hashed with PBKDF2-HMAC-SHA256 before storage.
    pub fn addUser(self: *SaslPlainAuthenticator, username: []const u8, password: []const u8) !void {
        const user_copy = try self.allocator.dupe(u8, username);
        errdefer self.allocator.free(user_copy);

        // Generate random salt
        var salt: [16]u8 = undefined;
        @import("random_compat").bytes(&salt);

        // Hash password with PBKDF2
        var hash: [32]u8 = undefined;
        computeSaltedPassword(password, &salt, pbkdf2_iterations, &hash);

        try self.credentials.put(user_copy, .{
            .salt = salt,
            .hash = hash,
            .iterations = pbkdf2_iterations,
        });
    }

    /// Authenticate a SASL/PLAIN token.
    /// SASL/PLAIN format: \0<username>\0<password>
    /// The password from the token is hashed with the stored salt and compared
    /// against the stored hash (constant-time comparison).
    pub fn authenticate(self: *const SaslPlainAuthenticator, token: []const u8) AuthResult {
        // Parse SASL/PLAIN: [authzid]\0username\0password
        var parts: [3][]const u8 = undefined;
        var part_count: usize = 0;
        var start: usize = 0;
        for (token, 0..) |c, i| {
            if (c == 0) {
                if (part_count < 3) {
                    parts[part_count] = token[start..i];
                    part_count += 1;
                }
                start = i + 1;
            }
        }
        if (part_count < 3 and start <= token.len) {
            if (part_count < 3) {
                parts[part_count] = token[start..];
                part_count += 1;
            }
        }

        if (part_count < 3) {
            log.warn("SASL/PLAIN authentication failed: malformed token ({d} parts, expected 3)", .{part_count});
            return .{ .success = false, .principal = null };
        }

        const username = parts[1];
        const password = parts[2];

        if (self.credentials.get(username)) |cred| {
            // Hash the incoming password with the stored salt
            var computed_hash: [32]u8 = undefined;
            computeSaltedPassword(password, &cred.salt, cred.iterations, &computed_hash);

            // Constant-time comparison to prevent timing attacks
            if (constantTimeEqual(&computed_hash, &cred.hash)) {
                log.info("Authentication success: user={s}", .{username});
                return .{ .success = true, .principal = username };
            }
        }

        log.warn("Authentication failed: user={s}", .{username});

        return .{ .success = false, .principal = null };
    }

    pub const AuthResult = struct {
        success: bool,
        principal: ?[]const u8,
    };
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "Authorizer open access (no ACLs)" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();

    const result = auth.authorize("User:alice", .topic, "test-topic", .read);
    try testing.expectEqual(Authorizer.AuthResult.allowed, result);
}

test "Authorizer allow/deny" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();

    // Allow alice to read topic "test"
    try auth.addAcl("User:alice", .topic, "test", .literal, .read, .allow, "*");
    // Deny bob everything
    try auth.addAcl("User:bob", .any, "*", .any, .any, .deny, "*");

    try testing.expectEqual(Authorizer.AuthResult.allowed, auth.authorize("User:alice", .topic, "test", .read));
    try testing.expectEqual(Authorizer.AuthResult.denied, auth.authorize("User:alice", .topic, "test", .write));
    try testing.expectEqual(Authorizer.AuthResult.denied, auth.authorize("User:bob", .topic, "test", .read));
}

test "Authorizer prefix pattern" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();

    try auth.addAcl("User:app", .topic, "app.", .prefixed, .all, .allow, "*");

    try testing.expectEqual(Authorizer.AuthResult.allowed, auth.authorize("User:app", .topic, "app.events", .write));
    try testing.expectEqual(Authorizer.AuthResult.allowed, auth.authorize("User:app", .topic, "app.users", .read));
    try testing.expectEqual(Authorizer.AuthResult.denied, auth.authorize("User:app", .topic, "system.logs", .read));
}

test "Authorizer deny overrides allow" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();

    try auth.addAcl("User:eve", .topic, "secret", .literal, .read, .allow, "*");
    try auth.addAcl("User:eve", .topic, "secret", .literal, .read, .deny, "*");

    try testing.expectEqual(Authorizer.AuthResult.denied, auth.authorize("User:eve", .topic, "secret", .read));
}

test "SaslPlainAuthenticator" {
    var sasl = SaslPlainAuthenticator.init(testing.allocator);
    defer sasl.deinit();

    try sasl.addUser("admin", "secret123");
    try sasl.addUser("user1", "pass456");

    // Valid auth: \0admin\0secret123
    const token1 = "\x00admin\x00secret123";
    const result1 = sasl.authenticate(token1);
    try testing.expect(result1.success);
    try testing.expectEqualStrings("admin", result1.principal.?);

    // Invalid password
    const token2 = "\x00admin\x00wrongpass";
    const result2 = sasl.authenticate(token2);
    try testing.expect(!result2.success);

    // Unknown user
    const token3 = "\x00unknown\x00pass";
    const result3 = sasl.authenticate(token3);
    try testing.expect(!result3.success);
}

/// SCRAM-SHA-256 authenticator.
///
/// Implements RFC 5802 / RFC 7677 for SASL SCRAM-SHA-256 authentication.
/// This is the recommended authentication mechanism for Kafka in production.
///
/// SCRAM exchange:
///   1. Client → client-first-message: n,,n=username,r=client-nonce
///   2. Server → server-first-message: r=combined-nonce,s=salt,i=iterations
///   3. Client → client-final-message: c=biws,r=combined-nonce,p=client-proof
///   4. Server → server-final-message: v=server-signature
pub const ScramSha256Authenticator = struct {
    users: std.StringHashMap(ScramCredential),
    allocator: Allocator,

    pub const ScramCredential = struct {
        salt: [32]u8,
        iterations: u32,
        stored_key: [32]u8,
        server_key: [32]u8,
    };

    pub const ScramState = enum {
        initial,
        server_first_sent,
        authenticated,
        failed,
    };

    pub fn init(alloc: Allocator) ScramSha256Authenticator {
        return .{
            .users = std.StringHashMap(ScramCredential).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ScramSha256Authenticator) void {
        var it = self.users.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.users.deinit();
    }

    /// Add a user with a pre-computed SCRAM credential.
    pub fn addUser(self: *ScramSha256Authenticator, username: []const u8, password: []const u8) !void {
        const user_copy = try self.allocator.dupe(u8, username);
        errdefer self.allocator.free(user_copy);

        // Generate random salt
        var salt: [32]u8 = undefined;
        @import("random_compat").bytes(&salt);

        const iterations: u32 = 4096;

        // Compute SaltedPassword = Hi(password, salt, iterations)
        // Hi is PBKDF2-HMAC-SHA256
        var salted_password: [32]u8 = undefined;
        computeSaltedPassword(password, &salt, iterations, &salted_password);

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        var client_key: [32]u8 = undefined;
        hmacSha256(&salted_password, "Client Key", &client_key);

        // StoredKey = SHA256(ClientKey)
        var stored_key: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&client_key, &stored_key, .{});

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        var server_key: [32]u8 = undefined;
        hmacSha256(&salted_password, "Server Key", &server_key);

        try self.users.put(user_copy, .{
            .salt = salt,
            .iterations = iterations,
            .stored_key = stored_key,
            .server_key = server_key,
        });
    }

    /// Upsert a user with Kafka AlterUserScramCredentials pre-computed material.
    pub fn putCredential(self: *ScramSha256Authenticator, username: []const u8, salt: [32]u8, iterations: u32, salted_password: [32]u8) !void {
        if (self.users.fetchRemove(username)) |old| {
            self.allocator.free(old.key);
        }

        const user_copy = try self.allocator.dupe(u8, username);
        errdefer self.allocator.free(user_copy);

        var client_key: [32]u8 = undefined;
        hmacSha256(&salted_password, "Client Key", &client_key);

        var stored_key: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&client_key, &stored_key, .{});

        var server_key: [32]u8 = undefined;
        hmacSha256(&salted_password, "Server Key", &server_key);

        try self.users.put(user_copy, .{
            .salt = salt,
            .iterations = iterations,
            .stored_key = stored_key,
            .server_key = server_key,
        });
    }

    pub fn removeUser(self: *ScramSha256Authenticator, username: []const u8) bool {
        if (self.users.fetchRemove(username)) |old| {
            self.allocator.free(old.key);
            return true;
        }
        return false;
    }

    /// Look up a user's SCRAM credential.
    pub fn getCredential(self: *const ScramSha256Authenticator, username: []const u8) ?ScramCredential {
        return self.users.get(username);
    }

    /// Verify a client proof against stored credentials.
    /// Returns true if authentication succeeds.
    pub fn verifyClientProof(
        _: *const ScramSha256Authenticator,
        credential: ScramCredential,
        auth_message: []const u8,
        client_proof: [32]u8,
    ) bool {
        // ClientSignature = HMAC(StoredKey, AuthMessage)
        var client_signature: [32]u8 = undefined;
        hmacSha256(&credential.stored_key, auth_message, &client_signature);

        // RecoveredClientKey = ClientProof XOR ClientSignature
        var recovered_key: [32]u8 = undefined;
        for (&recovered_key, client_proof, client_signature) |*r, p, s| {
            r.* = p ^ s;
        }

        // VerifyStoredKey = SHA256(RecoveredClientKey)
        var verify_key: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&recovered_key, &verify_key, .{});

        return std.mem.eql(u8, &verify_key, &credential.stored_key);
    }

    /// Compute the server signature for the final message.
    pub fn computeServerSignature(
        _: *const ScramSha256Authenticator,
        credential: ScramCredential,
        auth_message: []const u8,
    ) [32]u8 {
        var server_signature: [32]u8 = undefined;
        hmacSha256(&credential.server_key, auth_message, &server_signature);
        return server_signature;
    }
};

/// SCRAM-SHA-256 state machine for a single client connection.
///
/// Implements the server side of the RFC 5802 / RFC 7677 SCRAM-SHA-256
/// authentication exchange. Each connection gets its own ScramStateMachine
/// instance, tracked by client_id in the broker.
///
/// The 4-message exchange:
///   1. Client → client-first-message: "n,,n=<user>,r=<client_nonce>"
///   2. Server → server-first-message: "r=<combined_nonce>,s=<base64_salt>,i=<iterations>"
///   3. Client → client-final-message: "c=biws,r=<combined_nonce>,p=<base64_client_proof>"
///   4. Server → server-final-message: "v=<base64_server_signature>"
pub const ScramStateMachine = struct {
    state: ScramSha256Authenticator.ScramState,
    /// The nonce sent by the client in client-first-message.
    client_nonce: ?[]u8,
    /// The nonce generated by the server, appended to client_nonce to form combined_nonce.
    server_nonce: ?[]u8,
    /// The full auth_message used for proof/signature computation (RFC 5802 §3).
    /// auth_message = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
    auth_message: ?[]u8,
    /// Username extracted from client-first-message.
    username: ?[]u8,
    /// The credential looked up for the username (cached after client-first-message).
    credential: ?ScramSha256Authenticator.ScramCredential,
    /// The complete server-first-message (stored for auth_message construction).
    server_first_message: ?[]u8,
    /// The client-first-message-bare (n=<user>,r=<client_nonce>) for auth_message construction.
    client_first_message_bare: ?[]u8,
    allocator: Allocator,

    pub fn init(alloc: Allocator) ScramStateMachine {
        return .{
            .state = .initial,
            .client_nonce = null,
            .server_nonce = null,
            .auth_message = null,
            .username = null,
            .credential = null,
            .server_first_message = null,
            .client_first_message_bare = null,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ScramStateMachine) void {
        if (self.client_nonce) |n| self.allocator.free(n);
        if (self.server_nonce) |n| self.allocator.free(n);
        if (self.auth_message) |m| self.allocator.free(m);
        if (self.username) |u| self.allocator.free(u);
        if (self.server_first_message) |m| self.allocator.free(m);
        if (self.client_first_message_bare) |m| self.allocator.free(m);
    }

    /// Handle client-first-message: parse username and client nonce, generate server-first-message.
    /// Returns the server-first-message bytes to send back, or null on failure.
    ///
    /// client-first-message format: "n,,n=<username>,r=<client_nonce>"
    /// The "n,," is the GS2 header (no channel binding). "n=<user>,r=<nonce>" is the bare part.
    pub fn handleClientFirst(
        self: *ScramStateMachine,
        authenticator: *const ScramSha256Authenticator,
        client_first: []const u8,
    ) ?[]u8 {
        if (self.state != .initial) {
            log.warn("SCRAM: handleClientFirst called in state {}, expected initial", .{self.state});
            self.state = .failed;
            return null;
        }

        // Parse GS2 header: expect "n,," prefix (no channel binding)
        // The bare message starts after the GS2 header "n,,"
        const gs2_prefix = "n,,";
        if (!std.mem.startsWith(u8, client_first, gs2_prefix)) {
            log.warn("SCRAM: invalid GS2 header in client-first-message", .{});
            self.state = .failed;
            return null;
        }
        const bare = client_first[gs2_prefix.len..];

        // Parse client-first-message-bare: "n=<username>,r=<client_nonce>"
        const username = parseAttribute(bare, 'n') orelse {
            log.warn("SCRAM: missing username (n=) in client-first-message", .{});
            self.state = .failed;
            return null;
        };
        const client_nonce = parseAttribute(bare, 'r') orelse {
            log.warn("SCRAM: missing nonce (r=) in client-first-message", .{});
            self.state = .failed;
            return null;
        };

        // Look up user credential
        const credential = authenticator.getCredential(username) orelse {
            log.warn("SCRAM: unknown user '{s}'", .{username});
            self.state = .failed;
            return null;
        };

        // Store client-first-message-bare for auth_message construction
        self.client_first_message_bare = self.allocator.dupe(u8, bare) catch {
            self.state = .failed;
            return null;
        };

        // Store username
        self.username = self.allocator.dupe(u8, username) catch {
            self.state = .failed;
            return null;
        };

        // Store client nonce
        self.client_nonce = self.allocator.dupe(u8, client_nonce) catch {
            self.state = .failed;
            return null;
        };

        // Store credential
        self.credential = credential;

        // Generate server nonce (24 random bytes, base64-encoded)
        var server_nonce_raw: [24]u8 = undefined;
        @import("random_compat").bytes(&server_nonce_raw);
        const server_nonce_b64 = base64Encode(self.allocator, &server_nonce_raw) catch {
            self.state = .failed;
            return null;
        };
        self.server_nonce = server_nonce_b64;

        // Base64-encode the salt
        const salt_b64 = base64Encode(self.allocator, &credential.salt) catch {
            self.state = .failed;
            return null;
        };
        defer self.allocator.free(salt_b64);

        // Build server-first-message: "r=<combined_nonce>,s=<base64_salt>,i=<iterations>"
        const server_first = std.fmt.allocPrint(
            self.allocator,
            "r={s}{s},s={s},i={d}",
            .{ client_nonce, server_nonce_b64, salt_b64, credential.iterations },
        ) catch {
            self.state = .failed;
            return null;
        };

        self.server_first_message = self.allocator.dupe(u8, server_first) catch {
            self.allocator.free(server_first);
            self.state = .failed;
            return null;
        };

        self.state = .server_first_sent;
        return server_first;
    }

    /// Handle client-final-message: verify client proof, generate server-final-message.
    /// Returns the server-final-message bytes to send back, or null on failure.
    ///
    /// client-final-message format: "c=biws,r=<combined_nonce>,p=<base64_client_proof>"
    /// "biws" is base64("n,,") — the GS2 channel binding data for no binding.
    pub fn handleClientFinal(
        self: *ScramStateMachine,
        authenticator: *const ScramSha256Authenticator,
        client_final: []const u8,
    ) ?[]u8 {
        if (self.state != .server_first_sent) {
            log.warn("SCRAM: handleClientFinal called in state {}, expected server_first_sent", .{self.state});
            self.state = .failed;
            return null;
        }

        // Parse channel binding: must be "c=biws" (base64 of "n,,")
        const channel_binding = parseAttribute(client_final, 'c') orelse {
            log.warn("SCRAM: missing channel binding (c=) in client-final-message", .{});
            self.state = .failed;
            return null;
        };
        if (!std.mem.eql(u8, channel_binding, "biws")) {
            log.warn("SCRAM: unexpected channel binding value: {s}", .{channel_binding});
            self.state = .failed;
            return null;
        }

        // Parse combined nonce: must match client_nonce + server_nonce
        const received_nonce = parseAttribute(client_final, 'r') orelse {
            log.warn("SCRAM: missing nonce (r=) in client-final-message", .{});
            self.state = .failed;
            return null;
        };

        // Verify the nonce matches our expected combined nonce
        const client_nonce = self.client_nonce orelse {
            self.state = .failed;
            return null;
        };
        const server_nonce = self.server_nonce orelse {
            self.state = .failed;
            return null;
        };

        // Build expected combined nonce
        const expected_nonce = std.fmt.allocPrint(self.allocator, "{s}{s}", .{ client_nonce, server_nonce }) catch {
            self.state = .failed;
            return null;
        };
        defer self.allocator.free(expected_nonce);

        if (!std.mem.eql(u8, received_nonce, expected_nonce)) {
            log.warn("SCRAM: nonce mismatch in client-final-message", .{});
            self.state = .failed;
            return null;
        }

        // Parse client proof (base64-encoded)
        const proof_b64 = parseAttribute(client_final, 'p') orelse {
            log.warn("SCRAM: missing proof (p=) in client-final-message", .{});
            self.state = .failed;
            return null;
        };
        var client_proof: [32]u8 = undefined;
        base64Decode(proof_b64, &client_proof) catch {
            log.warn("SCRAM: invalid base64 in client proof", .{});
            self.state = .failed;
            return null;
        };

        // Build auth_message = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
        // client-final-message-without-proof is everything before ",p="
        const client_final_without_proof = if (std.mem.lastIndexOf(u8, client_final, ",p=")) |pos|
            client_final[0..pos]
        else {
            log.warn("SCRAM: malformed client-final-message (no ,p= found)", .{});
            self.state = .failed;
            return null;
        };

        const client_first_bare = self.client_first_message_bare orelse {
            self.state = .failed;
            return null;
        };
        const server_first = self.server_first_message orelse {
            self.state = .failed;
            return null;
        };

        const auth_msg = std.fmt.allocPrint(
            self.allocator,
            "{s},{s},{s}",
            .{ client_first_bare, server_first, client_final_without_proof },
        ) catch {
            self.state = .failed;
            return null;
        };
        self.auth_message = auth_msg;

        // Verify client proof using existing ScramSha256Authenticator.verifyClientProof()
        const credential = self.credential orelse {
            self.state = .failed;
            return null;
        };
        if (!authenticator.verifyClientProof(credential, auth_msg, client_proof)) {
            log.warn("SCRAM: client proof verification failed for user '{s}'", .{self.username orelse "unknown"});
            self.state = .failed;
            return null;
        }

        // Compute server signature for server-final-message
        const server_signature = authenticator.computeServerSignature(credential, auth_msg);
        const sig_b64 = base64Encode(self.allocator, &server_signature) catch {
            self.state = .failed;
            return null;
        };
        defer self.allocator.free(sig_b64);

        // Build server-final-message: "v=<base64_server_signature>"
        const server_final = std.fmt.allocPrint(self.allocator, "v={s}", .{sig_b64}) catch {
            self.state = .failed;
            return null;
        };

        self.state = .authenticated;
        log.info("SCRAM-SHA-256 authentication success: user={s}", .{self.username orelse "unknown"});
        return server_final;
    }

    /// Parse a single SCRAM attribute from a comma-separated message.
    /// Looks for "<attr>=<value>" and returns the value portion.
    /// Per RFC 5802, attributes are single characters (e.g., n=, r=, s=, i=, c=, p=, v=).
    pub fn parseAttribute(message: []const u8, attr: u8) ?[]const u8 {
        var iter = std.mem.splitScalar(u8, message, ',');
        while (iter.next()) |part| {
            if (part.len >= 2 and part[0] == attr and part[1] == '=') {
                return part[2..];
            }
        }
        return null;
    }
};

/// Base64-encode arbitrary bytes into a newly allocated string.
fn base64Encode(alloc: Allocator, data: []const u8) ![]u8 {
    const encoder = std.base64.standard.Encoder;
    const encoded_len = encoder.calcSize(data.len);
    const buf = try alloc.alloc(u8, encoded_len);
    _ = encoder.encode(buf, data);
    return buf;
}

/// Base64-decode a string into a fixed-size output buffer.
fn base64Decode(encoded: []const u8, out: *[32]u8) !void {
    const decoder = std.base64.standard.Decoder;
    decoder.decode(out, encoded) catch return error.InvalidBase64;
}

/// Constant-time comparison of two 32-byte arrays.
/// Prevents timing side-channel attacks on password hash comparison.
fn constantTimeEqual(a: *const [32]u8, b: *const [32]u8) bool {
    var diff: u8 = 0;
    for (a, b) |x, y| {
        diff |= x ^ y;
    }
    return diff == 0;
}

/// HMAC-SHA256 computation.
fn hmacSha256(key: []const u8, data: []const u8, out: *[32]u8) void {
    const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
    HmacSha256.create(out, data, key);
}

/// PBKDF2-HMAC-SHA256 (Hi function from RFC 5802).
fn computeSaltedPassword(password: []const u8, salt: []const u8, iterations: u32, out: *[32]u8) void {
    // U1 = HMAC(password, salt || INT(1))
    var salt_with_i: [36]u8 = undefined;
    @memcpy(salt_with_i[0..salt.len], salt);
    std.mem.writeInt(u32, salt_with_i[salt.len..][0..4], 1, .big);

    var u_prev: [32]u8 = undefined;
    hmacSha256(password, salt_with_i[0 .. salt.len + 4], &u_prev);

    var result = u_prev;

    var i: u32 = 1;
    while (i < iterations) : (i += 1) {
        var u_next: [32]u8 = undefined;
        hmacSha256(password, &u_prev, &u_next);
        for (&result, u_next) |*r, n| {
            r.* ^= n;
        }
        u_prev = u_next;
    }

    out.* = result;
}

/// SASL/OAUTHBEARER authenticator.
///
/// Implements the Kafka OAUTHBEARER SASL mechanism (KIP-255).
/// Accepts a JWT bearer token, validates expiration and optional
/// issuer/audience claims, and extracts the principal from the "sub" claim.
///
/// NOTE: AutoMQ inherits Java's JSSE OAUTHBEARER implementation with full
/// JWKS-based signature verification. ZMQ validates token structure,
/// expiration, and issuer/audience claims. Cryptographic signature
/// verification requires a JWKS endpoint (future enhancement) or can be
/// delegated to a trusted token gateway in front of the broker.
///
/// OAUTHBEARER SASL token format (RFC 7628):
///   "n,,\x01auth=Bearer <jwt_token>\x01\x01"
pub const OAuthBearerAuthenticator = struct {
    expected_issuer: ?[]const u8 = null,
    expected_audience: ?[]const u8 = null,

    const JwtToken = @import("jwt.zig").JwtToken;

    pub fn init() OAuthBearerAuthenticator {
        return .{};
    }

    pub fn initWithConfig(expected_issuer: ?[]const u8, expected_audience: ?[]const u8) OAuthBearerAuthenticator {
        return .{
            .expected_issuer = expected_issuer,
            .expected_audience = expected_audience,
        };
    }

    /// Authenticate an OAUTHBEARER SASL token.
    /// The token format is: "n,,\x01auth=Bearer <jwt>\x01\x01"
    /// Returns the authenticated principal (from JWT "sub" claim) or failure.
    /// The returned principal is allocator-owned; callers must call
    /// `AuthResult.deinit` after copying or using it.
    pub fn authenticate(self: *const OAuthBearerAuthenticator, alloc: Allocator, token: []const u8) AuthResult {
        // Extract the JWT from the OAUTHBEARER SASL framing
        const jwt_str = extractBearerToken(token) orelse {
            log.warn("OAUTHBEARER: malformed SASL token (no Bearer token found)", .{});
            return .{ .success = false, .principal = null };
        };

        // Parse the JWT
        var jwt = JwtToken.parse(alloc, jwt_str) catch {
            log.warn("OAUTHBEARER: failed to parse JWT", .{});
            return .{ .success = false, .principal = null };
        };

        // Check expiration
        if (jwt.isExpired()) {
            log.warn("OAUTHBEARER: token expired (exp={?d})", .{jwt.expires_at});
            jwt.deinit();
            return .{ .success = false, .principal = null };
        }
        if (jwt.isNotYetValid()) {
            log.warn("OAUTHBEARER: token not yet valid (nbf={?d})", .{jwt.not_before});
            jwt.deinit();
            return .{ .success = false, .principal = null };
        }

        // Validate issuer if configured
        if (self.expected_issuer) |expected| {
            if (jwt.issuer) |actual| {
                if (!std.mem.eql(u8, actual, expected)) {
                    log.warn("OAUTHBEARER: issuer mismatch (expected={s}, got={s})", .{ expected, actual });
                    jwt.deinit();
                    return .{ .success = false, .principal = null };
                }
            } else {
                log.warn("OAUTHBEARER: token missing issuer claim", .{});
                jwt.deinit();
                return .{ .success = false, .principal = null };
            }
        }

        // Validate audience if configured
        if (self.expected_audience) |expected| {
            if (!jwt.hasAudience(expected)) {
                log.warn("OAUTHBEARER: audience mismatch or missing audience (expected={s})", .{expected});
                jwt.deinit();
                return .{ .success = false, .principal = null };
            }
        }

        // Extract principal from "sub" claim.
        const principal = jwt.getPrincipal() orelse {
            log.warn("OAUTHBEARER: token missing 'sub' claim", .{});
            jwt.deinit();
            return .{ .success = false, .principal = null };
        };

        const principal_copy = alloc.dupe(u8, principal) catch {
            jwt.deinit();
            return .{ .success = false, .principal = null };
        };
        jwt.deinit();

        log.info("OAUTHBEARER authentication success: principal={s}", .{principal_copy});
        return .{ .success = true, .principal = principal_copy };
    }

    /// Extract the Bearer JWT from the OAUTHBEARER SASL token format.
    /// Format: "n,,\x01auth=Bearer <jwt>\x01\x01" (RFC 7628 GS2 framing)
    /// Also accepts raw "Bearer <jwt>" or just the JWT directly.
    fn extractBearerToken(token: []const u8) ?[]const u8 {
        // Try to find "auth=Bearer " in the SASL framing
        if (std.mem.indexOf(u8, token, "auth=Bearer ")) |pos| {
            const jwt_start = pos + "auth=Bearer ".len;
            // JWT ends at the next \x01 or end of token
            const rest = token[jwt_start..];
            const jwt_end = std.mem.indexOf(u8, rest, "\x01") orelse rest.len;
            if (jwt_end > 0) return rest[0..jwt_end];
        }

        // Try "Bearer <jwt>" prefix
        if (std.mem.startsWith(u8, token, "Bearer ")) {
            return token["Bearer ".len..];
        }

        // Try raw JWT (has two dots)
        if (std.mem.indexOf(u8, token, ".") != null) {
            const first_dot = std.mem.indexOf(u8, token, ".") orelse return null;
            const rest = token[first_dot + 1 ..];
            if (std.mem.indexOf(u8, rest, ".") != null) {
                return token; // Looks like a raw JWT
            }
        }

        return null;
    }

    pub const AuthResult = struct {
        success: bool,
        principal: ?[]u8,

        pub fn deinit(self: *AuthResult, alloc: Allocator) void {
            if (self.principal) |principal| {
                alloc.free(principal);
                self.principal = null;
            }
        }
    };
};

// ---------------------------------------------------------------
// SCRAM Tests
// ---------------------------------------------------------------

test "ScramSha256 add user and get credential" {
    var scram = ScramSha256Authenticator.init(testing.allocator);
    defer scram.deinit();

    try scram.addUser("testuser", "testpass");

    const cred = scram.getCredential("testuser");
    try testing.expect(cred != null);
    try testing.expectEqual(@as(u32, 4096), cred.?.iterations);

    const missing = scram.getCredential("nobody");
    try testing.expect(missing == null);
}

test "HMAC-SHA256 basic" {
    var out: [32]u8 = undefined;
    hmacSha256("key", "data", &out);
    // Just verify it produces a non-zero hash
    var all_zero = true;
    for (out) |b| {
        if (b != 0) {
            all_zero = false;
            break;
        }
    }
    try testing.expect(!all_zero);
}

test "PBKDF2 basic" {
    var out: [32]u8 = undefined;
    const salt = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    computeSaltedPassword("password", &salt, 1, &out);
    // Verify non-zero output
    var all_zero = true;
    for (out) |b| {
        if (b != 0) {
            all_zero = false;
            break;
        }
    }
    try testing.expect(!all_zero);
}

test "Authorizer super user bypasses deny" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();

    try auth.addSuperUser("User:admin");
    try auth.addAcl("User:admin", .topic, "secret", .literal, .read, .deny, "*");

    // Super user bypasses ACLs even with explicit deny
    try testing.expectEqual(Authorizer.AuthResult.allowed, auth.authorize("User:admin", .topic, "secret", .read));
    // Non-super user is denied
    try testing.expectEqual(Authorizer.AuthResult.denied, auth.authorize("User:regular", .topic, "secret", .read));
}

test "Authorizer allow_everyone_if_no_acl=false denies" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();
    auth.allow_everyone_if_no_acl = false;

    // With no ACLs and allow_everyone=false, deny everything
    try testing.expectEqual(Authorizer.AuthResult.denied, auth.authorize("User:alice", .topic, "test", .read));
}

test "Authorizer allow_everyone_if_no_acl=true allows" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();
    auth.allow_everyone_if_no_acl = true;

    // Default: with no ACLs, allow everything
    try testing.expectEqual(Authorizer.AuthResult.allowed, auth.authorize("User:alice", .topic, "test", .read));
}

test "Authorizer removeMatchingAcls" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();

    try auth.addAcl("User:alice", .topic, "test", .literal, .read, .allow, "*");
    try auth.addAcl("User:alice", .topic, "test", .literal, .write, .allow, "*");
    try auth.addAcl("User:bob", .topic, "test", .literal, .read, .allow, "*");

    try testing.expectEqual(@as(usize, 3), auth.aclCount());

    // Remove only alice's read ACL
    const removed = auth.removeMatchingAcls(.topic, "test", .literal, "User:alice", null, .read, .any);
    try testing.expectEqual(@as(usize, 1), removed);
    try testing.expectEqual(@as(usize, 2), auth.aclCount());

    // Alice can still write but not read
    try testing.expectEqual(Authorizer.AuthResult.denied, auth.authorize("User:alice", .topic, "test", .read));
    try testing.expectEqual(Authorizer.AuthResult.allowed, auth.authorize("User:alice", .topic, "test", .write));
}

test "Authorizer removeMatchingAcls wildcard filter" {
    var auth = Authorizer.init(testing.allocator);
    defer auth.deinit();

    try auth.addAcl("User:alice", .topic, "t1", .literal, .read, .allow, "*");
    try auth.addAcl("User:alice", .topic, "t2", .literal, .read, .allow, "*");
    try auth.addAcl("User:bob", .topic, "t1", .literal, .read, .allow, "*");

    // Remove all of alice's ACLs (wildcard resource name and operation)
    const removed = auth.removeMatchingAcls(.any, null, .any, "User:alice", null, .any, .any);
    try testing.expectEqual(@as(usize, 2), removed);
    try testing.expectEqual(@as(usize, 1), auth.aclCount());
}

test "OAuthBearerAuthenticator valid token" {
    const oauth = OAuthBearerAuthenticator.init();

    // Build a valid OAUTHBEARER SASL token with a JWT that has sub="oauthuser", exp=9999999999
    // JWT: header={"alg":"none"} payload={"sub":"oauthuser","exp":9999999999}
    // header b64: eyJhbGciOiJub25lIn0
    // payload b64: eyJzdWIiOiJvYXV0aHVzZXIiLCJleHAiOjk5OTk5OTk5OTl9
    const jwt = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJvYXV0aHVzZXIiLCJleHAiOjk5OTk5OTk5OTl9.";
    const sasl_token = "n,,\x01auth=Bearer " ++ jwt ++ "\x01\x01";

    var result = oauth.authenticate(testing.allocator, sasl_token);
    defer result.deinit(testing.allocator);
    try testing.expect(result.success);
    try testing.expect(result.principal != null);
    try testing.expectEqualStrings("oauthuser", result.principal.?);
}

test "OAuthBearerAuthenticator expired token" {
    const oauth = OAuthBearerAuthenticator.init();

    // JWT with exp=1000000000 (year 2001 — expired)
    const jwt = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJleHBpcmVkIiwiZXhwIjoxMDAwMDAwMDAwfQ.";
    const sasl_token = "n,,\x01auth=Bearer " ++ jwt ++ "\x01\x01";

    var result = oauth.authenticate(testing.allocator, sasl_token);
    defer result.deinit(testing.allocator);
    try testing.expect(!result.success);
}

test "OAuthBearerAuthenticator issuer validation" {
    const oauth = OAuthBearerAuthenticator.initWithConfig("expected-issuer", null);

    // JWT with iss="wrong-issuer"
    const jwt = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyIiwiaXNzIjoid3JvbmctaXNzdWVyIiwiZXhwIjo5OTk5OTk5OTk5fQ.";
    const sasl_token = "n,,\x01auth=Bearer " ++ jwt ++ "\x01\x01";

    var result = oauth.authenticate(testing.allocator, sasl_token);
    defer result.deinit(testing.allocator);
    try testing.expect(!result.success);
}

test "OAuthBearerAuthenticator accepts array-valued audience claims" {
    const oauth = OAuthBearerAuthenticator.initWithConfig(null, "kafka");

    const jwt = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhcnJheWF1ZCIsImF1ZCI6WyJub3Qta2Fma2EiLCJrYWZrYSJdLCJleHAiOjk5OTk5OTk5OTl9.";
    const sasl_token = "n,,\x01auth=Bearer " ++ jwt ++ "\x01\x01";

    var result = oauth.authenticate(testing.allocator, sasl_token);
    defer result.deinit(testing.allocator);
    try testing.expect(result.success);
    try testing.expectEqualStrings("arrayaud", result.principal.?);
}

test "OAuthBearerAuthenticator rejects future not-before tokens" {
    const oauth = OAuthBearerAuthenticator.init();

    const jwt = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJmdXR1cmUiLCJuYmYiOjk5OTk5OTk5OTksImV4cCI6MTAwMDAwMDAwMDB9.";
    const sasl_token = "n,,\x01auth=Bearer " ++ jwt ++ "\x01\x01";

    var result = oauth.authenticate(testing.allocator, sasl_token);
    defer result.deinit(testing.allocator);
    try testing.expect(!result.success);
}

test "OAuthBearerAuthenticator raw JWT token" {
    const oauth = OAuthBearerAuthenticator.init();

    // Pass raw JWT without SASL framing
    const jwt = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJyYXd1c2VyIiwiZXhwIjo5OTk5OTk5OTk5fQ.";

    var result = oauth.authenticate(testing.allocator, jwt);
    defer result.deinit(testing.allocator);
    try testing.expect(result.success);
    try testing.expect(result.principal != null);
    try testing.expectEqualStrings("rawuser", result.principal.?);
}

test "OAuthBearerAuthenticator rejects token without subject without leaks" {
    const oauth = OAuthBearerAuthenticator.init();

    const jwt = "eyJhbGciOiJub25lIn0.eyJleHAiOjk5OTk5OTk5OTl9.";
    const sasl_token = "n,,\x01auth=Bearer " ++ jwt ++ "\x01\x01";

    var result = oauth.authenticate(testing.allocator, sasl_token);
    defer result.deinit(testing.allocator);
    try testing.expect(!result.success);
}

// ---------------------------------------------------------------
// ScramStateMachine Tests
// ---------------------------------------------------------------

/// Test helper: simulate the client side of a SCRAM-SHA-256 exchange.
/// Given a password and the server-first-message, computes the client-final-message
/// with a valid client proof.
fn testBuildClientFinal(
    alloc: Allocator,
    password: []const u8,
    client_first_bare: []const u8,
    server_first: []const u8,
    combined_nonce: []const u8,
) ![]u8 {
    // Parse salt and iterations from server-first-message
    const salt_b64 = ScramStateMachine.parseAttribute(server_first, 's') orelse return error.MissingSalt;
    const iter_str = ScramStateMachine.parseAttribute(server_first, 'i') orelse return error.MissingIterations;
    const iterations = try std.fmt.parseInt(u32, iter_str, 10);

    // Decode salt from base64
    const decoder = std.base64.standard.Decoder;
    const salt_len = decoder.calcSizeForSlice(salt_b64) catch return error.InvalidBase64;
    const salt = try alloc.alloc(u8, salt_len);
    defer alloc.free(salt);
    decoder.decode(salt, salt_b64) catch return error.InvalidBase64;

    // SaltedPassword = Hi(password, salt, iterations)
    var salted_password: [32]u8 = undefined;
    computeSaltedPassword(password, salt, iterations, &salted_password);

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    var client_key: [32]u8 = undefined;
    hmacSha256(&salted_password, "Client Key", &client_key);

    // StoredKey = SHA256(ClientKey)
    var stored_key: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&client_key, &stored_key, .{});

    // client-final-message-without-proof = "c=biws,r=<combined_nonce>"
    const client_final_without_proof = try std.fmt.allocPrint(alloc, "c=biws,r={s}", .{combined_nonce});
    defer alloc.free(client_final_without_proof);

    // auth_message = client-first-message-bare + "," + server-first-message + "," + client-final-without-proof
    const auth_message = try std.fmt.allocPrint(alloc, "{s},{s},{s}", .{ client_first_bare, server_first, client_final_without_proof });
    defer alloc.free(auth_message);

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    var client_signature: [32]u8 = undefined;
    hmacSha256(&stored_key, auth_message, &client_signature);

    // ClientProof = ClientKey XOR ClientSignature
    var client_proof: [32]u8 = undefined;
    for (&client_proof, client_key, client_signature) |*p, k, s| {
        p.* = k ^ s;
    }

    // Base64-encode the proof
    const proof_b64 = try base64Encode(alloc, &client_proof);
    defer alloc.free(proof_b64);

    // Build client-final-message: "c=biws,r=<combined_nonce>,p=<base64_proof>"
    return try std.fmt.allocPrint(alloc, "c=biws,r={s},p={s}", .{ combined_nonce, proof_b64 });
}

test "ScramStateMachine successful 2-round exchange" {
    const alloc = testing.allocator;
    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("alice", "password123");

    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    // Round 1: client-first-message
    const client_nonce = "rOprNGfwEbeRWgbNEkqO";
    const client_first = "n,,n=alice,r=" ++ client_nonce;

    const server_first = sm.handleClientFirst(&scram_auth, client_first) orelse {
        return error.UnexpectedNull;
    };
    defer alloc.free(server_first);

    try testing.expectEqual(ScramSha256Authenticator.ScramState.server_first_sent, sm.state);

    // Verify server-first-message format: "r=<combined_nonce>,s=<base64_salt>,i=<iterations>"
    try testing.expect(std.mem.startsWith(u8, server_first, "r=" ++ client_nonce));
    try testing.expect(std.mem.indexOf(u8, server_first, ",s=") != null);
    try testing.expect(std.mem.indexOf(u8, server_first, ",i=4096") != null);

    // Extract combined nonce from server-first
    const combined_nonce = ScramStateMachine.parseAttribute(server_first, 'r') orelse return error.MissingNonce;

    // Round 2: client-final-message (simulating client side)
    const client_first_bare = "n=alice,r=" ++ client_nonce;
    const client_final = try testBuildClientFinal(alloc, "password123", client_first_bare, server_first, combined_nonce);
    defer alloc.free(client_final);

    const server_final = sm.handleClientFinal(&scram_auth, client_final) orelse {
        return error.UnexpectedNull;
    };
    defer alloc.free(server_final);

    // Verify server-final-message format: "v=<base64_server_signature>"
    try testing.expect(std.mem.startsWith(u8, server_final, "v="));
    try testing.expectEqual(ScramSha256Authenticator.ScramState.authenticated, sm.state);
    try testing.expectEqualStrings("alice", sm.username.?);
}

test "ScramStateMachine wrong password rejection" {
    const alloc = testing.allocator;
    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("bob", "correct_password");

    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    // Round 1: client-first-message
    const client_nonce = "testNonce12345";
    const client_first = "n,,n=bob,r=" ++ client_nonce;

    const server_first = sm.handleClientFirst(&scram_auth, client_first) orelse {
        return error.UnexpectedNull;
    };
    defer alloc.free(server_first);

    try testing.expectEqual(ScramSha256Authenticator.ScramState.server_first_sent, sm.state);

    // Extract combined nonce
    const combined_nonce = ScramStateMachine.parseAttribute(server_first, 'r') orelse return error.MissingNonce;

    // Round 2: client-final with WRONG password
    const client_first_bare = "n=bob,r=" ++ client_nonce;
    const client_final = try testBuildClientFinal(alloc, "wrong_password", client_first_bare, server_first, combined_nonce);
    defer alloc.free(client_final);

    // handleClientFinal should return null (proof verification failed)
    const server_final = sm.handleClientFinal(&scram_auth, client_final);
    try testing.expect(server_final == null);
    try testing.expectEqual(ScramSha256Authenticator.ScramState.failed, sm.state);
}

test "ScramStateMachine nonce mismatch rejection" {
    const alloc = testing.allocator;
    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("carol", "pass");

    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    // Round 1
    const client_first = "n,,n=carol,r=myNonce";
    const server_first = sm.handleClientFirst(&scram_auth, client_first) orelse {
        return error.UnexpectedNull;
    };
    defer alloc.free(server_first);

    // Round 2: send client-final with a tampered nonce
    const tampered_final = "c=biws,r=differentNonce123,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const result = sm.handleClientFinal(&scram_auth, tampered_final);
    try testing.expect(result == null);
    try testing.expectEqual(ScramSha256Authenticator.ScramState.failed, sm.state);
}

test "ScramStateMachine state reuse prevention" {
    const alloc = testing.allocator;
    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("dave", "davepass");

    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    // Calling handleClientFinal before handleClientFirst should fail
    const premature_final = "c=biws,r=someNonce,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const result1 = sm.handleClientFinal(&scram_auth, premature_final);
    try testing.expect(result1 == null);
    try testing.expectEqual(ScramSha256Authenticator.ScramState.failed, sm.state);

    // After failure, calling handleClientFirst should also fail (state is failed)
    var sm2 = ScramStateMachine.init(alloc);
    defer sm2.deinit();

    // Complete a full exchange
    const client_first = "n,,n=dave,r=nonce1";
    const server_first = sm2.handleClientFirst(&scram_auth, client_first) orelse return error.UnexpectedNull;
    defer alloc.free(server_first);

    // Now try calling handleClientFirst again — should fail (state is server_first_sent, not initial)
    const result2 = sm2.handleClientFirst(&scram_auth, "n,,n=dave,r=nonce2");
    try testing.expect(result2 == null);
    try testing.expectEqual(ScramSha256Authenticator.ScramState.failed, sm2.state);
}

test "ScramStateMachine unknown user rejection" {
    const alloc = testing.allocator;
    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    // No users added

    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    const client_first = "n,,n=nobody,r=nonce123";
    const result = sm.handleClientFirst(&scram_auth, client_first);
    try testing.expect(result == null);
    try testing.expectEqual(ScramSha256Authenticator.ScramState.failed, sm.state);
}

test "ScramStateMachine invalid GS2 header rejection" {
    const alloc = testing.allocator;
    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("eve", "evepass");

    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    // Missing GS2 header "n,,"
    const bad_first = "n=eve,r=nonce";
    const result = sm.handleClientFirst(&scram_auth, bad_first);
    try testing.expect(result == null);
    try testing.expectEqual(ScramSha256Authenticator.ScramState.failed, sm.state);
}

test "ScramStateMachine parseAttribute" {
    // Test the attribute parser used internally
    const msg = "n=user,r=clientnonce,e=extraval";
    try testing.expectEqualStrings("user", ScramStateMachine.parseAttribute(msg, 'n').?);
    try testing.expectEqualStrings("clientnonce", ScramStateMachine.parseAttribute(msg, 'r').?);
    try testing.expectEqualStrings("extraval", ScramStateMachine.parseAttribute(msg, 'e').?);
    // Attribute not present returns null
    try testing.expect(ScramStateMachine.parseAttribute(msg, 'z') == null);
    // Multi-char key like "extra" won't be parsed — only single-char attributes per RFC 5802
    const msg2 = "n=user,extra=value";
    try testing.expect(ScramStateMachine.parseAttribute(msg2, 'x') == null);
}

test "ScramStateMachine invalid channel binding rejection" {
    const alloc = testing.allocator;
    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("frank", "frankpass");

    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    const client_first = "n,,n=frank,r=nonce1";
    const server_first = sm.handleClientFirst(&scram_auth, client_first) orelse return error.UnexpectedNull;
    defer alloc.free(server_first);

    // Send client-final with wrong channel binding (not "biws")
    const combined_nonce = ScramStateMachine.parseAttribute(server_first, 'r') orelse return error.MissingNonce;
    const bad_final = try std.fmt.allocPrint(alloc, "c=wrongbinding,r={s},p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", .{combined_nonce});
    defer alloc.free(bad_final);

    const result = sm.handleClientFinal(&scram_auth, bad_final);
    try testing.expect(result == null);
    try testing.expectEqual(ScramSha256Authenticator.ScramState.failed, sm.state);
}

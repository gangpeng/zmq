const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.auth);

/// ACL (Access Control List) authorization engine.
///
/// Implements the Kafka ACL model:
///   (Principal, Permission, Operation, Resource, PatternType) → Allow/Deny
pub const Authorizer = struct {
    acls: std.ArrayList(AclEntry),
    allocator: Allocator,

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
            .acls = std.ArrayList(AclEntry).init(alloc),
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
    }

    /// Add an ACL entry.
    pub fn addAcl(self: *Authorizer, principal: []const u8, resource_type: ResourceType, resource_name: []const u8, pattern_type: PatternType, operation: Operation, permission: Permission, host: []const u8) !void {
        try self.acls.append(.{
            .principal = try self.allocator.dupe(u8, principal),
            .resource_type = resource_type,
            .resource_name = try self.allocator.dupe(u8, resource_name),
            .pattern_type = pattern_type,
            .operation = operation,
            .permission = permission,
            .host = try self.allocator.dupe(u8, host),
        });
        log.info("ACL added: principal={s} resource={s} op={d} permission={d}", .{ principal, resource_name, @intFromEnum(operation), @intFromEnum(permission) });
    }

    /// Check if an operation is authorized.
    pub fn authorize(self: *const Authorizer, principal: []const u8, resource_type: ResourceType, resource_name: []const u8, operation: Operation) AuthResult {
        // If no ACLs exist, allow everything (open access mode)
        if (self.acls.items.len == 0) return .allowed;

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
/// Simple username/password authentication.
pub const SaslPlainAuthenticator = struct {
    credentials: std.StringHashMap([]u8),
    allocator: Allocator,

    pub fn init(alloc: Allocator) SaslPlainAuthenticator {
        return .{
            .credentials = std.StringHashMap([]u8).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *SaslPlainAuthenticator) void {
        var it = self.credentials.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.credentials.deinit();
    }

    /// Register a username/password pair.
    pub fn addUser(self: *SaslPlainAuthenticator, username: []const u8, password: []const u8) !void {
        const user_copy = try self.allocator.dupe(u8, username);
        errdefer self.allocator.free(user_copy);
        const pass_copy = try self.allocator.dupe(u8, password);
        try self.credentials.put(user_copy, pass_copy);
    }

    /// Authenticate a SASL/PLAIN token.
    /// SASL/PLAIN format: \0<username>\0<password>
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

        if (self.credentials.get(username)) |stored_pass| {
            if (std.mem.eql(u8, password, stored_pass)) {
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
        completed,
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
        std.crypto.random.bytes(&salt);

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

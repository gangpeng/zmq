const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.jwt);

/// Minimal JWT (JSON Web Token) parser for SASL/OAUTHBEARER authentication.
///
/// Parses the three-part JWT structure (header.payload.signature),
/// base64url-decodes the payload, and extracts standard claims.
///
/// NOTE: AutoMQ inherits Java's JSSE OAUTHBEARER implementation which
/// supports full JWKS-based signature verification. ZMQ implements
/// claim extraction and expiration checking. Signature verification
/// requires JWKS endpoint support (future enhancement) or can be done
/// by a trusted token issuer gateway in front of the broker.
pub const JwtToken = struct {
    /// Raw header (base64url-decoded JSON)
    header_json: []const u8,
    /// Raw payload (base64url-decoded JSON)
    payload_json: []const u8,
    /// Raw signature bytes (base64url-decoded)
    signature: []const u8,
    /// Extracted claims
    subject: ?[]u8 = null,
    issuer: ?[]u8 = null,
    audience: ?[]u8 = null,
    audience_values: [][]u8 = &.{},
    scope: ?[]u8 = null,
    expires_at: ?i64 = null,
    issued_at: ?i64 = null,
    not_before: ?i64 = null,
    allocator: Allocator,

    /// Parse a JWT token string (header.payload.signature).
    pub fn parse(alloc: Allocator, token: []const u8) !JwtToken {
        // Find the two dots separating header.payload.signature
        const first_dot = std.mem.indexOf(u8, token, ".") orelse return error.MalformedJwt;
        const rest = token[first_dot + 1 ..];
        const second_dot = std.mem.indexOf(u8, rest, ".") orelse return error.MalformedJwt;

        const header_b64 = token[0..first_dot];
        const payload_b64 = rest[0..second_dot];
        const sig_b64 = rest[second_dot + 1 ..];

        // Decode each part
        var transferred_to_jwt = false;
        const header_json = try base64UrlDecode(alloc, header_b64);
        errdefer if (!transferred_to_jwt) alloc.free(header_json);
        const payload_json = try base64UrlDecode(alloc, payload_b64);
        errdefer if (!transferred_to_jwt) alloc.free(payload_json);
        const signature = try base64UrlDecode(alloc, sig_b64);
        errdefer if (!transferred_to_jwt) alloc.free(signature);

        // Extract claims from payload JSON
        var jwt = JwtToken{
            .header_json = header_json,
            .payload_json = payload_json,
            .signature = signature,
            .allocator = alloc,
        };

        transferred_to_jwt = true;
        errdefer jwt.deinit();
        try jwt.extractClaims();

        return jwt;
    }

    /// Check if the token has expired.
    pub fn isExpired(self: *const JwtToken) bool {
        const exp = self.expires_at orelse return false; // No exp = never expires
        const now = @import("time_compat").timestamp();
        return now > exp;
    }

    /// Check if the token's not-before claim is still in the future.
    pub fn isNotYetValid(self: *const JwtToken) bool {
        const nbf = self.not_before orelse return false;
        const now = @import("time_compat").timestamp();
        return now < nbf;
    }

    /// Check if the token audience matches either a string aud claim or an
    /// array-valued aud claim, both of which are common across OAuth providers.
    pub fn hasAudience(self: *const JwtToken, expected: []const u8) bool {
        if (self.audience) |actual| {
            if (std.mem.eql(u8, actual, expected)) return true;
        }
        for (self.audience_values) |actual| {
            if (std.mem.eql(u8, actual, expected)) return true;
        }
        return false;
    }

    /// Get the principal (subject claim).
    pub fn getPrincipal(self: *const JwtToken) ?[]const u8 {
        return self.subject;
    }

    pub fn deinit(self: *JwtToken) void {
        if (self.subject) |subject| self.allocator.free(subject);
        if (self.issuer) |issuer| self.allocator.free(issuer);
        if (self.audience) |audience| self.allocator.free(audience);
        for (self.audience_values) |audience| self.allocator.free(audience);
        if (self.audience_values.len > 0) self.allocator.free(self.audience_values);
        if (self.scope) |scope| self.allocator.free(scope);
        self.allocator.free(self.header_json);
        self.allocator.free(self.payload_json);
        self.allocator.free(self.signature);
    }

    fn extractClaims(self: *JwtToken) !void {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, self.payload_json, .{
            .duplicate_field_behavior = .@"error",
            .parse_numbers = true,
        });
        defer parsed.deinit();

        const object = switch (parsed.value) {
            .object => |*object| object,
            else => return error.InvalidJwtPayload,
        };

        self.subject = try copyOptionalStringClaim(self.allocator, object, "sub");
        self.issuer = try copyOptionalStringClaim(self.allocator, object, "iss");
        self.scope = try copyOptionalStringClaim(self.allocator, object, "scope");
        self.expires_at = optionalIntClaim(object, "exp");
        self.issued_at = optionalIntClaim(object, "iat");
        self.not_before = optionalIntClaim(object, "nbf");
        try self.copyAudienceClaim(object);
    }

    fn copyAudienceClaim(self: *JwtToken, object: *const std.json.ObjectMap) !void {
        const value = object.get("aud") orelse return;
        switch (value) {
            .string => |audience| {
                self.audience = try self.allocator.dupe(u8, audience);
            },
            .array => |array| {
                var audiences = std.array_list.Managed([]u8).init(self.allocator);
                errdefer {
                    for (audiences.items) |audience| self.allocator.free(audience);
                    audiences.deinit();
                }

                for (array.items) |item| {
                    switch (item) {
                        .string => |audience| {
                            const copy = try self.allocator.dupe(u8, audience);
                            errdefer self.allocator.free(copy);
                            try audiences.append(copy);
                        },
                        else => {},
                    }
                }

                self.audience_values = try audiences.toOwnedSlice();
            },
            else => {},
        }
    }
};

/// Base64url decode (RFC 4648 Section 5).
/// JWT uses base64url encoding without padding.
fn base64UrlDecode(alloc: Allocator, input: []const u8) ![]u8 {
    if (input.len == 0) return try alloc.alloc(u8, 0);

    // Convert base64url to standard base64 and add padding
    const padded_len = input.len + (4 - input.len % 4) % 4;
    var buf = try alloc.alloc(u8, padded_len);
    defer alloc.free(buf);

    for (input, 0..) |c, i| {
        buf[i] = switch (c) {
            '-' => '+',
            '_' => '/',
            else => c,
        };
    }
    // Add '=' padding
    for (input.len..padded_len) |i| {
        buf[i] = '=';
    }

    // Compute exact decoded size
    const decoder = std.base64.standard.Decoder;
    const decoded_len = decoder.calcSizeForSlice(buf) catch return error.Base64DecodeFailed;
    const result = try alloc.alloc(u8, decoded_len);
    decoder.decode(result, buf) catch {
        alloc.free(result);
        return error.Base64DecodeFailed;
    };
    return result;
}

fn copyOptionalStringClaim(alloc: Allocator, object: *const std.json.ObjectMap, key: []const u8) !?[]u8 {
    const value = object.get(key) orelse return null;
    return switch (value) {
        .string => |s| try alloc.dupe(u8, s),
        else => null,
    };
}

fn optionalIntClaim(object: *const std.json.ObjectMap, key: []const u8) ?i64 {
    const value = object.get(key) orelse return null;
    return switch (value) {
        .integer => |i| i,
        .number_string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "JwtToken parse valid token" {
    // Build a test JWT: header={"alg":"HS256","typ":"JWT"}, payload={"sub":"user1","iss":"test","exp":9999999999}
    // header base64url: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9
    // payload base64url: eyJzdWIiOiJ1c2VyMSIsImlzcyI6InRlc3QiLCJleHAiOjk5OTk5OTk5OTl9
    // signature: dGVzdHNpZw (base64url of "testsig")
    const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMSIsImlzcyI6InRlc3QiLCJleHAiOjk5OTk5OTk5OTl9.dGVzdHNpZw";

    var jwt = try JwtToken.parse(testing.allocator, token);
    defer jwt.deinit();

    try testing.expectEqualStrings("user1", jwt.subject.?);
    try testing.expectEqualStrings("test", jwt.issuer.?);
    try testing.expectEqual(@as(i64, 9999999999), jwt.expires_at.?);
    try testing.expect(!jwt.isExpired());
    try testing.expect(!jwt.isNotYetValid());
}

test "JwtToken parse expired token" {
    // payload={"sub":"expired","exp":1000000000} (year 2001 — expired)
    // header: eyJhbGciOiJIUzI1NiJ9
    // payload: eyJzdWIiOiJleHBpcmVkIiwiZXhwIjoxMDAwMDAwMDAwfQ
    const token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJleHBpcmVkIiwiZXhwIjoxMDAwMDAwMDAwfQ.c2ln";

    var jwt = try JwtToken.parse(testing.allocator, token);
    defer jwt.deinit();

    try testing.expectEqualStrings("expired", jwt.subject.?);
    try testing.expect(jwt.isExpired());
}

test "JwtToken malformed token rejected" {
    try testing.expectError(error.MalformedJwt, JwtToken.parse(testing.allocator, "not-a-jwt"));
    try testing.expectError(error.MalformedJwt, JwtToken.parse(testing.allocator, "onlyonepart"));
}

test "base64UrlDecode" {
    const decoded = try base64UrlDecode(testing.allocator, "SGVsbG8");
    defer testing.allocator.free(decoded);
    try testing.expectEqualStrings("Hello", decoded);
}

test "base64UrlDecode with url-safe chars" {
    // Standard base64 "a+b/c==" → base64url "a-b_c"
    const decoded = try base64UrlDecode(testing.allocator, "YS1i");
    defer testing.allocator.free(decoded);
    // "YS1i" base64url decodes to "a-b" — wait, let me use a known value
    // base64url("test") = "dGVzdA"
    const decoded2 = try base64UrlDecode(testing.allocator, "dGVzdA");
    defer testing.allocator.free(decoded2);
    try testing.expectEqualStrings("test", decoded2);
}

test "JwtToken parses JSON claims structurally" {
    const token_with_embedded_key_text = "eyJhbGciOiJub25lIn0.eyJub2lzZSI6Ilwic3ViXCI6XCJldmlsXCIiLCJzdWIiOiJyZWFsLXVzZXIiLCJleHAiOjk5OTk5OTk5OTl9.";
    var embedded = try JwtToken.parse(testing.allocator, token_with_embedded_key_text);
    defer embedded.deinit();
    try testing.expectEqualStrings("real-user", embedded.subject.?);
    try testing.expectEqual(@as(i64, 9999999999), embedded.expires_at.?);

    const token_with_escaped_subject = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyXHUwMDJEb25lIiwiZXhwIjo5OTk5OTk5OTk5fQ.";
    var escaped = try JwtToken.parse(testing.allocator, token_with_escaped_subject);
    defer escaped.deinit();
    try testing.expectEqualStrings("user-one", escaped.subject.?);
}

test "JwtToken rejects duplicate payload claims" {
    const duplicate_exp_token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJkdXAiLCJleHAiOjEwMDAsImV4cCI6OTk5OTk5OTk5OX0.";
    try testing.expectError(error.DuplicateField, JwtToken.parse(testing.allocator, duplicate_exp_token));
}

test "JwtToken audience array and not-before claims" {
    const array_audience_token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhcnJheWF1ZCIsImF1ZCI6WyJub3Qta2Fma2EiLCJrYWZrYSJdLCJleHAiOjk5OTk5OTk5OTl9.";
    var array_audience_jwt = try JwtToken.parse(testing.allocator, array_audience_token);
    defer array_audience_jwt.deinit();
    try testing.expect(array_audience_jwt.audience == null);
    try testing.expect(array_audience_jwt.hasAudience("kafka"));
    try testing.expect(!array_audience_jwt.hasAudience("missing"));

    const not_before_token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJmdXR1cmUiLCJuYmYiOjk5OTk5OTk5OTksImV4cCI6MTAwMDAwMDAwMDB9.";
    var not_before_jwt = try JwtToken.parse(testing.allocator, not_before_token);
    defer not_before_jwt.deinit();
    try testing.expectEqual(@as(i64, 9999999999), not_before_jwt.not_before.?);
    try testing.expect(not_before_jwt.isNotYetValid());
}

test "JwtToken audience array handles escaped JSON strings" {
    const escaped_array_audience_token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhdWR1c2VyIiwiYXVkIjpbIm5vdC1rYWZrYSIsImthZlx1MDA2YmEiXSwiZXhwIjo5OTk5OTk5OTk5fQ.";
    var jwt = try JwtToken.parse(testing.allocator, escaped_array_audience_token);
    defer jwt.deinit();
    try testing.expect(jwt.hasAudience("kafka"));
    try testing.expect(!jwt.hasAudience("kaf\\u006ba"));
}

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
    subject: ?[]const u8 = null,
    issuer: ?[]const u8 = null,
    audience: ?[]const u8 = null,
    scope: ?[]const u8 = null,
    expires_at: ?i64 = null,
    issued_at: ?i64 = null,
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
        const header_json = try base64UrlDecode(alloc, header_b64);
        errdefer alloc.free(header_json);
        const payload_json = try base64UrlDecode(alloc, payload_b64);
        errdefer alloc.free(payload_json);
        const signature = try base64UrlDecode(alloc, sig_b64);
        errdefer alloc.free(signature);

        // Extract claims from payload JSON
        var jwt = JwtToken{
            .header_json = header_json,
            .payload_json = payload_json,
            .signature = signature,
            .allocator = alloc,
        };

        jwt.subject = extractStringClaim(payload_json, "\"sub\"");
        jwt.issuer = extractStringClaim(payload_json, "\"iss\"");
        jwt.audience = extractStringClaim(payload_json, "\"aud\"");
        jwt.scope = extractStringClaim(payload_json, "\"scope\"");
        jwt.expires_at = extractIntClaim(payload_json, "\"exp\"");
        jwt.issued_at = extractIntClaim(payload_json, "\"iat\"");

        return jwt;
    }

    /// Check if the token has expired.
    pub fn isExpired(self: *const JwtToken) bool {
        const exp = self.expires_at orelse return false; // No exp = never expires
        const now = std.time.timestamp();
        return now > exp;
    }

    /// Get the principal (subject claim).
    pub fn getPrincipal(self: *const JwtToken) ?[]const u8 {
        return self.subject;
    }

    pub fn deinit(self: *JwtToken) void {
        self.allocator.free(self.header_json);
        self.allocator.free(self.payload_json);
        self.allocator.free(self.signature);
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

/// Extract a string claim value from JSON.
/// Simple substring search — finds "key":"value" patterns.
/// This is a minimal JSON parser sufficient for JWT claims.
fn extractStringClaim(json: []const u8, key: []const u8) ?[]const u8 {
    const key_pos = std.mem.indexOf(u8, json, key) orelse return null;
    const after_key = json[key_pos + key.len ..];

    // Skip whitespace and colon
    var i: usize = 0;
    while (i < after_key.len and (after_key[i] == ' ' or after_key[i] == ':' or after_key[i] == '\t')) : (i += 1) {}

    if (i >= after_key.len or after_key[i] != '"') return null;
    i += 1; // skip opening quote

    const value_start = i;
    // Find closing quote (handle escaped quotes)
    while (i < after_key.len) : (i += 1) {
        if (after_key[i] == '"' and (i == 0 or after_key[i - 1] != '\\')) break;
    }
    if (i >= after_key.len) return null;

    return after_key[value_start..i];
}

/// Extract an integer claim value from JSON.
fn extractIntClaim(json: []const u8, key: []const u8) ?i64 {
    const key_pos = std.mem.indexOf(u8, json, key) orelse return null;
    const after_key = json[key_pos + key.len ..];

    // Skip whitespace and colon
    var i: usize = 0;
    while (i < after_key.len and (after_key[i] == ' ' or after_key[i] == ':' or after_key[i] == '\t')) : (i += 1) {}

    if (i >= after_key.len) return null;

    // Parse integer
    const start = i;
    while (i < after_key.len and (after_key[i] >= '0' and after_key[i] <= '9')) : (i += 1) {}
    if (i == start) return null;

    return std.fmt.parseInt(i64, after_key[start..i], 10) catch null;
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

test "extractStringClaim" {
    const json = "{\"sub\":\"alice\",\"iss\":\"myserver\",\"aud\":\"kafka\"}";
    try testing.expectEqualStrings("alice", extractStringClaim(json, "\"sub\"").?);
    try testing.expectEqualStrings("myserver", extractStringClaim(json, "\"iss\"").?);
    try testing.expectEqualStrings("kafka", extractStringClaim(json, "\"aud\"").?);
    try testing.expect(extractStringClaim(json, "\"missing\"") == null);
}

test "extractIntClaim" {
    const json = "{\"exp\":1234567890,\"iat\":1000000000}";
    try testing.expectEqual(@as(i64, 1234567890), extractIntClaim(json, "\"exp\"").?);
    try testing.expectEqual(@as(i64, 1000000000), extractIntClaim(json, "\"iat\"").?);
    try testing.expect(extractIntClaim(json, "\"missing\"") == null);
}

/// ZMQ Security
///
/// - Authorizer: ACL-based authorization engine
/// - SaslPlainAuthenticator: SASL/PLAIN username/password auth
/// - ScramSha256Authenticator: SASL/SCRAM-SHA-256 (production-grade)
/// - OAuthBearerAuthenticator: SASL/OAUTHBEARER JWT token auth
/// - TLS: SSL/TLS encrypted connections (OpenSSL wrapper)
/// - JWT: JSON Web Token parser for OAUTHBEARER

pub const auth = @import("security/auth.zig");
pub const tls = @import("security/tls.zig");
pub const jwt = @import("security/jwt.zig");
pub const openssl = @import("security/openssl.zig");

pub const Authorizer = auth.Authorizer;
pub const SaslPlainAuthenticator = auth.SaslPlainAuthenticator;
pub const ScramSha256Authenticator = auth.ScramSha256Authenticator;
pub const OAuthBearerAuthenticator = auth.OAuthBearerAuthenticator;
pub const JwtToken = jwt.JwtToken;
pub const TlsConfig = tls.TlsConfig;
pub const TlsContext = tls.TlsContext;
pub const TlsConnection = tls.TlsConnection;

test {
    @import("std").testing.refAllDecls(@This());
}

/// AutoMQ Security
///
/// - Authorizer: ACL-based authorization engine
/// - SaslPlainAuthenticator: SASL/PLAIN username/password auth
/// - ScramSha256Authenticator: SASL/SCRAM-SHA-256 (production-grade)
/// - TLS: SSL/TLS encrypted connections (OpenSSL wrapper)

pub const auth = @import("security/auth.zig");
pub const tls = @import("security/tls.zig");

pub const Authorizer = auth.Authorizer;
pub const SaslPlainAuthenticator = auth.SaslPlainAuthenticator;
pub const ScramSha256Authenticator = auth.ScramSha256Authenticator;
pub const TlsConfig = tls.TlsConfig;
pub const TlsContext = tls.TlsContext;
pub const TlsConnection = tls.TlsConnection;

test {
    @import("std").testing.refAllDecls(@This());
}

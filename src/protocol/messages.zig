/// Protocol message types.
///
/// Each file implements the request and response for one API key,
/// with serialize/deserialize/calcSize/deinit for all supported versions.

pub const api_versions = @import("messages/api_versions.zig");
pub const metadata = @import("messages/metadata.zig");

pub const ApiVersionsRequest = api_versions.ApiVersionsRequest;
pub const ApiVersionsResponse = api_versions.ApiVersionsResponse;
pub const MetadataRequest = metadata.MetadataRequest;
pub const MetadataResponse = metadata.MetadataResponse;

test {
    @import("std").testing.refAllDecls(@This());
}

/// ZMQ Wire Protocol
///
/// Kafka binary protocol implementation including:
/// - Serialization primitives (compact/non-compact strings, arrays, bytes, tagged fields)
/// - Request/Response headers (v0 and v1)
/// - API key registry (all 74 standard + 18 AutoMQ-compatible extensions)
/// - Protocol message types (ApiVersions, Metadata, etc.)
/// - Record batch format (V0, V1, V2)
/// - 230 auto-generated message structs with serialize/deserialize/calcSize
pub const api_key = @import("protocol/api_key.zig");
pub const api_support = @import("protocol/api_support.zig");
pub const serialization = @import("protocol/serialization.zig");
pub const header = @import("protocol/header.zig");
pub const messages = @import("protocol/messages.zig");
pub const record_batch = @import("protocol/record_batch.zig");
pub const generated = @import("protocol/generated_index.zig");
pub const generated_roundtrip = @import("protocol/generated_roundtrip.zig");
pub const error_codes = @import("protocol/error_codes.zig");

pub const ApiKey = api_key.ApiKey;
pub const ApiSupport = api_support;
pub const RequestHeader = header.RequestHeader;
pub const ResponseHeader = header.ResponseHeader;
pub const TaggedField = serialization.TaggedField;
pub const ErrorCode = error_codes.ErrorCode;

pub const ApiVersionsRequest = messages.ApiVersionsRequest;
pub const ApiVersionsResponse = messages.ApiVersionsResponse;
pub const MetadataRequest = messages.MetadataRequest;
pub const MetadataResponse = messages.MetadataResponse;

pub const RecordBatch = record_batch;
pub const RecordBatchHeader = record_batch.RecordBatchHeader;
pub const Record = record_batch.Record;

test {
    @import("std").testing.refAllDecls(@This());
}

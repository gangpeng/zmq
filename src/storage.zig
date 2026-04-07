/// AutoMQ Storage Engine
///
/// Multi-tier storage system:
/// - WAL: durable local writes with fsync and crash recovery
/// - LogCache: FIFO in-memory cache for uncommitted/recent data
/// - S3BlockCache: LRU cache for committed S3 blocks
/// - S3 Object Format: DataBlocks + IndexBlock + Footer
/// - Stream: partition data lifecycle (epoch, ranges, ownership)
/// - ObjectManager: S3 object metadata registry + fetch resolution
/// - CompactionManager: split multi-stream objects, merge small objects
/// - S3Client: HTTP client for MinIO/S3 with AWS SigV4 authentication
/// - S3Storage: unified interface (mock or real)
/// - MockS3: in-memory S3 mock for testing
/// - AwsSigV4: AWS Signature Version 4 request signing

pub const wal = @import("storage/wal.zig");
pub const cache = @import("storage/cache.zig");
pub const s3 = @import("storage/s3.zig");
pub const s3_client = @import("storage/s3_client.zig");
pub const aws_sigv4 = @import("storage/aws_sigv4.zig");
pub const stream = @import("storage/stream.zig");
pub const compaction = @import("storage/compaction.zig");

pub const Wal = wal.Wal;
pub const MemoryWal = wal.MemoryWal;
pub const LogCache = cache.LogCache;
pub const S3BlockCache = cache.S3BlockCache;
pub const MockS3 = s3.MockS3;
pub const ObjectWriter = s3.ObjectWriter;
pub const ObjectReader = s3.ObjectReader;
pub const S3Client = s3_client.S3Client;
pub const S3Storage = s3_client.S3Storage;
pub const AwsSigV4 = aws_sigv4.AwsSigV4;
pub const Stream = stream.Stream;
pub const ObjectManager = stream.ObjectManager;
pub const CompactionManager = compaction.CompactionManager;

test {
    @import("std").testing.refAllDecls(@This());
}

const std = @import("std");
const testing = std.testing;

/// MetricRegistry is defined in core to avoid circular dependencies between
/// broker, storage, and raft modules. Re-exported here for backward compatibility.
pub const MetricRegistry = @import("../core/metric_registry.zig").MetricRegistry;

/// Register standard broker metrics.
pub fn registerBrokerMetrics(registry: *MetricRegistry) !void {
    try registry.registerCounter("kafka_server_requests_total", "Total number of requests processed");
    try registry.registerCounter("kafka_server_bytes_in_total", "Total bytes received");
    try registry.registerCounter("kafka_server_bytes_out_total", "Total bytes sent");
    try registry.registerGauge("kafka_server_active_connections", "Number of active connections");
    try registry.registerGauge("kafka_server_partition_count", "Number of partitions");
    try registry.registerHistogram("kafka_server_request_latency_seconds", "Request processing latency");
    try registry.registerCounter("kafka_server_produce_requests_total", "Total produce requests");
    try registry.registerCounter("kafka_server_fetch_requests_total", "Total fetch requests");
    // Per-API latency histograms
    try registry.registerHistogram("kafka_server_produce_latency_seconds", "Produce request latency");
    try registry.registerHistogram("kafka_server_fetch_latency_seconds", "Fetch request latency");
    // Group/topic gauges (used by tick())
    try registry.registerGauge("kafka_server_group_count", "Number of consumer groups");
    try registry.registerGauge("kafka_server_topic_count", "Number of topics");
    try registry.registerGauge("kafka_server_member_count", "Number of consumer group members");
}

/// Register S3 I/O metrics (labeled by operation type).
pub fn registerS3Metrics(registry: *MetricRegistry) !void {
    try registry.registerLabeledCounter("s3_requests_total", "Total S3 requests", &.{"operation"});
    try registry.registerLabeledCounter("s3_request_errors_total", "Failed S3 requests", &.{"operation"});
    try registry.registerLabeledHistogram("s3_request_duration_seconds", "S3 request latency", &.{"operation"});
    try registry.registerLabeledCounter("s3_bytes_total", "Total S3 bytes transferred", &.{"direction"});
}

/// Register compaction metrics.
pub fn registerCompactionMetrics(registry: *MetricRegistry) !void {
    try registry.registerCounter("compaction_cycles_total", "Total compaction cycles completed");
    try registry.registerHistogram("compaction_cycle_duration_seconds", "Duration of compaction cycles");
    try registry.registerCounter("compaction_splits_total", "Total SSOs split into SOs");
    try registry.registerCounter("compaction_merges_total", "Total merge operations");
    try registry.registerCounter("compaction_cleanups_total", "Total cleanup deletions");
    try registry.registerCounter("compaction_errors_total", "Failed compaction operations");
    try registry.registerGauge("compaction_orphaned_keys", "Orphaned S3 keys pending retry");
}

/// Register cache metrics (labeled by cache type and result).
pub fn registerCacheMetrics(registry: *MetricRegistry) !void {
    try registry.registerLabeledCounter("cache_operations_total", "Cache lookup operations", &.{ "cache", "result" });
    try registry.registerLabeledCounter("cache_evictions_total", "Cache evictions", &.{"cache"});
    try registry.registerGauge("log_cache_size_bytes", "LogCache current memory usage");
    try registry.registerGauge("log_cache_entries", "LogCache current entry count");
    try registry.registerGauge("s3_block_cache_size_bytes", "S3BlockCache current memory usage");
    try registry.registerGauge("s3_block_cache_entries", "S3BlockCache current entry count");
}

/// Register Raft consensus metrics.
pub fn registerRaftMetrics(registry: *MetricRegistry) !void {
    try registry.registerCounter("raft_elections_started_total", "Elections started");
    try registry.registerCounter("raft_pre_votes_started_total", "Pre-vote rounds started");
    try registry.registerCounter("raft_votes_granted_total", "Votes granted to other candidates");
    try registry.registerCounter("raft_votes_rejected_total", "Votes rejected");
    try registry.registerCounter("raft_leader_elections_won_total", "Times this node became leader");
    try registry.registerCounter("raft_epoch_changes_total", "Epoch transitions");
    try registry.registerCounter("raft_log_entries_appended_total", "Entries appended to Raft log");
    try registry.registerCounter("raft_log_entries_committed_total", "Entries committed");
    try registry.registerCounter("raft_snapshots_taken_total", "Snapshot compactions");
    try registry.registerGauge("raft_current_epoch", "Current Raft epoch");
    try registry.registerGauge("raft_commit_index", "Current Raft commit index");
    try registry.registerGauge("raft_role", "Current Raft role (0=unattached, 1=follower, 2=candidate, 3=leader)");
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "registerBrokerMetrics" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerBrokerMetrics(&registry);
    try testing.expect(registry.counters.contains("kafka_server_requests_total"));
    try testing.expect(registry.gauges.contains("kafka_server_active_connections"));
    try testing.expect(registry.histograms.contains("kafka_server_request_latency_seconds"));
}

test "registerS3Metrics" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerS3Metrics(&registry);
    try testing.expect(registry.labeled_counter_meta.contains("s3_requests_total"));
    try testing.expect(registry.labeled_counter_meta.contains("s3_request_errors_total"));
    try testing.expect(registry.labeled_histogram_meta.contains("s3_request_duration_seconds"));
    try testing.expect(registry.labeled_counter_meta.contains("s3_bytes_total"));
}

test "registerCompactionMetrics" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerCompactionMetrics(&registry);
    try testing.expect(registry.counters.contains("compaction_cycles_total"));
    try testing.expect(registry.histograms.contains("compaction_cycle_duration_seconds"));
    try testing.expect(registry.counters.contains("compaction_splits_total"));
    try testing.expect(registry.gauges.contains("compaction_orphaned_keys"));
}

test "registerCacheMetrics" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerCacheMetrics(&registry);
    try testing.expect(registry.labeled_counter_meta.contains("cache_operations_total"));
    try testing.expect(registry.labeled_counter_meta.contains("cache_evictions_total"));
    try testing.expect(registry.gauges.contains("log_cache_size_bytes"));
    try testing.expect(registry.gauges.contains("s3_block_cache_size_bytes"));
}

test "registerRaftMetrics" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerRaftMetrics(&registry);
    try testing.expect(registry.counters.contains("raft_elections_started_total"));
    try testing.expect(registry.counters.contains("raft_votes_granted_total"));
    try testing.expect(registry.gauges.contains("raft_current_epoch"));
    try testing.expect(registry.gauges.contains("raft_role"));
}

const std = @import("std");
const testing = std.testing;
const api_support = @import("protocol").api_support;

/// MetricRegistry is defined in core to avoid circular dependencies between
/// broker, storage, and raft modules. Re-exported here for backward compatibility.
pub const MetricRegistry = @import("core").MetricRegistry;

fn registerCounterIfMissing(registry: *MetricRegistry, name: []const u8, help: []const u8) !void {
    if (!registry.counters.contains(name)) {
        try registry.registerCounter(name, help);
    }
}

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
    try registry.registerCounter("kafka_server_produce_throttle_total", "Total throttled produce requests");
    try registry.registerCounter("kafka_server_fetch_throttle_total", "Total throttled fetch requests");
    for (api_support.broker_supported_apis) |api| {
        try registerCounterIfMissing(registry, api.metric, "Total requests for a broker API");
    }
    // Per-API latency histograms
    try registry.registerHistogram("kafka_server_produce_latency_seconds", "Produce request latency");
    try registry.registerHistogram("kafka_server_fetch_latency_seconds", "Fetch request latency");
    // Group/topic gauges (used by tick())
    try registry.registerGauge("kafka_server_group_count", "Number of consumer groups");
    try registry.registerGauge("kafka_server_topic_count", "Number of topics");
    try registry.registerGauge("kafka_server_member_count", "Number of consumer group members");
    try registry.registerGauge("Kafka_server_connection_count", "AutoMQ-compatible active connection count");
    try registry.registerGauge("Kafka_topic_count", "AutoMQ-compatible topic count");
    try registry.registerGauge("Kafka_group_count", "AutoMQ-compatible consumer group count");
    try registry.registerGauge("Kafka_partition_count", "AutoMQ-compatible local partition count");
    try registry.registerGauge("Kafka_partition_total_count", "AutoMQ-compatible cluster partition count");
    try registry.registerGauge("kafka_controller_kafkacontroller_activecontrollercount", "JMX-compatible active controller count");
    try registry.registerGauge("kafka_controller_kafkacontroller_globaltopiccount", "JMX-compatible cluster-wide topic count observed by the controller");
    try registry.registerGauge("kafka_controller_kafkacontroller_globalpartitioncount", "JMX-compatible cluster-wide partition count observed by the controller");
    try registry.registerGauge("kafka_controller_kafkacontroller_offlinepartitionscount", "JMX-compatible offline partition count observed by the controller");
    try registry.registerGauge("kafka_controller_kafkacontroller_preferredreplicaimbalancecount", "JMX-compatible count of partitions whose current leader is not the preferred replica");
    try registry.registerGauge("kafka_log_logmanager_offlinelogdirectorycount", "JMX-compatible offline log directory count");
    try registry.registerCounter("kafka_server_replicamanager_failedisrupdatesperseccount_total", "JMX-compatible total failed ISR update events");
    try registry.registerGauge("kafka_server_kafkaserver_brokerstate", "JMX-compatible broker lifecycle state");
    try registry.registerGauge("kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent", "JMX-compatible request handler idle fraction");
    try registry.registerGauge("kafka_network_socketserver_networkprocessoravgidlepercent", "JMX-compatible network processor idle fraction");
    try registry.registerGauge("kafka_server_replicamanager_partitioncount", "JMX-compatible local partition count");
    try registry.registerGauge("kafka_server_replicamanager_leadercount", "JMX-compatible local leader partition count");
    try registry.registerGauge("kafka_server_replicamanager_underreplicatedpartitions", "JMX-compatible under-replicated partition count");
    try registry.registerGauge("kafka_server_replicamanager_underminisrpartitioncount", "JMX-compatible under-min-ISR partition count");
    try registry.registerGauge("kafka_server_replicamanager_atminisrpartitioncount", "JMX-compatible at-min-ISR partition count");
    try registry.registerGauge("kafka_server_replicamanager_offlinepartitionscount", "JMX-compatible offline partition count");
    try registry.registerGauge("kafka_server_replicamanager_reassigningpartitions", "JMX-compatible active partition reassignment count");
    try registry.registerCounter("kafka_server_replicamanager_isrshrinks_total", "JMX-compatible ISR shrink events");
    try registry.registerCounter("kafka_server_replicamanager_isrexpands_total", "JMX-compatible ISR expand events");
    try registry.registerGauge("kafka_network_requestchannel_requestqueuesize", "JMX-compatible request channel queue depth");
    try registry.registerGauge("kafka_network_requestchannel_responsequeuesize", "JMX-compatible response channel queue depth");
    try registry.registerLabeledGauge(
        "kafka_server_delayedoperationpurgatory_purgatorysize",
        "JMX-compatible delayed operation purgatory size",
        &.{"delayed_operation"},
    );
    try registry.registerCounter("kafka_server_brokertopicmetrics_totalproducerequests_total", "JMX-compatible total produce requests");
    try registry.registerCounter("kafka_server_brokertopicmetrics_totalfetchrequests_total", "JMX-compatible total fetch requests");
    try registry.registerCounter("kafka_server_brokertopicmetrics_messagesin_total", "JMX-compatible produced record count");
    try registry.registerCounter("kafka_server_brokertopicmetrics_bytesin_total", "JMX-compatible broker bytes in");
    try registry.registerCounter("kafka_server_brokertopicmetrics_bytesout_total", "JMX-compatible broker bytes out");
    try registry.registerCounter("kafka_server_brokertopicmetrics_bytesrejected_total", "JMX-compatible rejected produce bytes");
    try registry.registerCounter("kafka_server_brokertopicmetrics_failedproducerequests_total", "JMX-compatible failed produce requests");
    try registry.registerCounter("kafka_server_brokertopicmetrics_failedfetchrequests_total", "JMX-compatible failed fetch requests");
    try registry.registerLabeledCounter(
        "Kafka_request_count_total",
        "AutoMQ-compatible Kafka request count by API type and version",
        &.{ "type", "version" },
    );
    try registry.registerLabeledCounter(
        "Kafka_request_size_bytes_total",
        "AutoMQ-compatible Kafka request bytes by API type and version",
        &.{ "type", "version" },
    );
    try registry.registerLabeledCounter(
        "Kafka_response_size_bytes_total",
        "AutoMQ-compatible Kafka response bytes by API type and version",
        &.{ "type", "version" },
    );
    try registry.registerLabeledCounter(
        "Kafka_request_time_milliseconds_total",
        "AutoMQ-compatible Kafka request handling time by API type and version",
        &.{ "type", "version" },
    );
    try registry.registerLabeledCounter(
        "Kafka_request_error_count_total",
        "AutoMQ-compatible Kafka request result count by API type, version, and error",
        &.{ "type", "version", "error" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_requests_total",
        "JMX-compatible Kafka network request count by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_requestbytes_total",
        "JMX-compatible Kafka network request bytes by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_responsebytes_total",
        "JMX-compatible Kafka network response bytes by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_totaltimems_total",
        "JMX-compatible Kafka network total request time by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_requestqueuetimems_total",
        "JMX-compatible Kafka network request queue time by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_localtimems_total",
        "JMX-compatible Kafka network local request handling time by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_remotetimems_total",
        "JMX-compatible Kafka network remote wait time by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_responsequeuetimems_total",
        "JMX-compatible Kafka network response queue time by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_responsesendtimems_total",
        "JMX-compatible Kafka network response send time by request type and version",
        &.{ "request", "version" },
    );
    try registry.registerLabeledCounter(
        "kafka_network_requestmetrics_errors_total",
        "JMX-compatible Kafka network request errors by request type, version, and error",
        &.{ "request", "version", "error" },
    );
    // Per-API error counter (labeled by api name and error code)
    try registry.registerLabeledCounter(
        "kafka_server_api_errors_total",
        "Total API errors by API name and error code",
        &.{ "api", "error_code" },
    );
    // Consumer lag gauge (labeled by group, topic, partition)
    try registry.registerLabeledGauge(
        "kafka_consumer_lag",
        "Consumer group lag per partition",
        &.{ "group", "topic", "partition" },
    );
    // Active network connections gauge
    try registry.registerGauge("kafka_network_connections_active", "Number of active network connections");
    try registry.registerCounter("kafka_client_telemetry_pushes_total", "Accepted client telemetry pushes");
    try registry.registerCounter("kafka_client_telemetry_terminations_total", "Accepted terminating client telemetry pushes");
    try registry.registerCounter("kafka_client_telemetry_exported_total", "Client telemetry pushes exported to an external sink");
    try registry.registerCounter("kafka_client_telemetry_export_errors_total", "Client telemetry export failures");
    try registry.registerCounter("kafka_client_telemetry_export_bytes_total", "Client telemetry bytes exported to an external sink");
    try registry.registerGauge("kafka_client_telemetry_samples", "Retained active client telemetry samples");
    try registry.registerGauge("kafka_client_telemetry_bytes", "Retained active client telemetry bytes");
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
    try registry.registerCounter("compaction_destroyed_total", "Total destroyed S3 objects physically deleted");
    try registry.registerCounter("compaction_expired_prepared_total", "Total expired prepared S3 objects");
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
    // Sprint 5: per-API error counter, consumer lag, active connections
    try testing.expect(registry.labeled_counter_meta.contains("Kafka_request_count_total"));
    try testing.expect(registry.labeled_counter_meta.contains("Kafka_request_error_count_total"));
    try testing.expect(registry.labeled_counter_meta.contains("Kafka_request_size_bytes_total"));
    try testing.expect(registry.labeled_counter_meta.contains("Kafka_response_size_bytes_total"));
    try testing.expect(registry.labeled_counter_meta.contains("Kafka_request_time_milliseconds_total"));
    try testing.expect(registry.gauges.contains("Kafka_server_connection_count"));
    try testing.expect(registry.gauges.contains("Kafka_topic_count"));
    try testing.expect(registry.gauges.contains("Kafka_group_count"));
    try testing.expect(registry.gauges.contains("Kafka_partition_count"));
    try testing.expect(registry.gauges.contains("Kafka_partition_total_count"));
    try testing.expect(registry.gauges.contains("kafka_controller_kafkacontroller_activecontrollercount"));
    try testing.expect(registry.gauges.contains("kafka_controller_kafkacontroller_globaltopiccount"));
    try testing.expect(registry.gauges.contains("kafka_controller_kafkacontroller_globalpartitioncount"));
    try testing.expect(registry.gauges.contains("kafka_controller_kafkacontroller_offlinepartitionscount"));
    try testing.expect(registry.gauges.contains("kafka_controller_kafkacontroller_preferredreplicaimbalancecount"));
    try testing.expect(registry.gauges.contains("kafka_log_logmanager_offlinelogdirectorycount"));
    try testing.expect(registry.counters.contains("kafka_server_replicamanager_failedisrupdatesperseccount_total"));
    try testing.expect(registry.gauges.contains("kafka_server_kafkaserver_brokerstate"));
    try testing.expect(registry.gauges.contains("kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent"));
    try testing.expect(registry.gauges.contains("kafka_network_socketserver_networkprocessoravgidlepercent"));
    try testing.expect(registry.gauges.contains("kafka_server_replicamanager_partitioncount"));
    try testing.expect(registry.gauges.contains("kafka_server_replicamanager_leadercount"));
    try testing.expect(registry.gauges.contains("kafka_server_replicamanager_underreplicatedpartitions"));
    try testing.expect(registry.gauges.contains("kafka_server_replicamanager_underminisrpartitioncount"));
    try testing.expect(registry.gauges.contains("kafka_server_replicamanager_atminisrpartitioncount"));
    try testing.expect(registry.gauges.contains("kafka_server_replicamanager_offlinepartitionscount"));
    try testing.expect(registry.gauges.contains("kafka_server_replicamanager_reassigningpartitions"));
    try testing.expect(registry.counters.contains("kafka_server_replicamanager_isrshrinks_total"));
    try testing.expect(registry.counters.contains("kafka_server_replicamanager_isrexpands_total"));
    try testing.expect(registry.gauges.contains("kafka_network_requestchannel_requestqueuesize"));
    try testing.expect(registry.gauges.contains("kafka_network_requestchannel_responsequeuesize"));
    try testing.expect(registry.labeled_gauge_meta.contains("kafka_server_delayedoperationpurgatory_purgatorysize"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_totalproducerequests_total"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_totalfetchrequests_total"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_messagesin_total"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_bytesin_total"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_bytesout_total"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_bytesrejected_total"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_failedproducerequests_total"));
    try testing.expect(registry.counters.contains("kafka_server_brokertopicmetrics_failedfetchrequests_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_requests_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_requestbytes_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_responsebytes_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_totaltimems_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_requestqueuetimems_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_localtimems_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_remotetimems_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_responsequeuetimems_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_responsesendtimems_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_network_requestmetrics_errors_total"));
    try testing.expect(registry.labeled_counter_meta.contains("kafka_server_api_errors_total"));
    try testing.expect(registry.labeled_gauge_meta.contains("kafka_consumer_lag"));
    try testing.expect(registry.gauges.contains("kafka_network_connections_active"));
    try testing.expect(registry.counters.contains("kafka_client_telemetry_pushes_total"));
    try testing.expect(registry.counters.contains("kafka_client_telemetry_terminations_total"));
    try testing.expect(registry.counters.contains("kafka_client_telemetry_exported_total"));
    try testing.expect(registry.counters.contains("kafka_client_telemetry_export_errors_total"));
    try testing.expect(registry.counters.contains("kafka_client_telemetry_export_bytes_total"));
    try testing.expect(registry.gauges.contains("kafka_client_telemetry_samples"));
    try testing.expect(registry.gauges.contains("kafka_client_telemetry_bytes"));
}

test "registerBrokerMetrics covers broker API metric catalog" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerBrokerMetrics(&registry);
    for (api_support.broker_supported_apis) |api| {
        try testing.expect(registry.counters.contains(api.metric));
    }
    try testing.expect(registry.counters.contains("kafka_server_produce_throttle_total"));
    try testing.expect(registry.counters.contains("kafka_server_fetch_throttle_total"));
}

test "registerBrokerMetrics new JMX gauges export with HELP and TYPE" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerBrokerMetrics(&registry);
    registry.setGauge("kafka_controller_kafkacontroller_globaltopiccount", 7.0);
    registry.setGauge("kafka_controller_kafkacontroller_globalpartitioncount", 21.0);
    registry.setGauge("kafka_controller_kafkacontroller_offlinepartitionscount", 0.0);
    registry.setGauge("kafka_controller_kafkacontroller_preferredreplicaimbalancecount", 0.0);
    registry.setGauge("kafka_log_logmanager_offlinelogdirectorycount", 0.0);
    registry.incrementCounter("kafka_server_replicamanager_failedisrupdatesperseccount_total");

    const output = try registry.exportPrometheus(testing.allocator);
    defer testing.allocator.free(output);

    const expected = [_][]const u8{
        "# HELP kafka_controller_kafkacontroller_globaltopiccount",
        "# TYPE kafka_controller_kafkacontroller_globaltopiccount gauge",
        "kafka_controller_kafkacontroller_globaltopiccount 7",
        "kafka_controller_kafkacontroller_globalpartitioncount 21",
        "kafka_controller_kafkacontroller_offlinepartitionscount 0",
        "kafka_controller_kafkacontroller_preferredreplicaimbalancecount 0",
        "kafka_log_logmanager_offlinelogdirectorycount 0",
        "# TYPE kafka_server_replicamanager_failedisrupdatesperseccount_total counter",
        "kafka_server_replicamanager_failedisrupdatesperseccount_total 1",
    };
    for (expected) |needle| {
        try testing.expect(std.mem.indexOf(u8, output, needle) != null);
    }
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
    try testing.expect(registry.counters.contains("compaction_merges_total"));
    try testing.expect(registry.counters.contains("compaction_cleanups_total"));
    try testing.expect(registry.counters.contains("compaction_destroyed_total"));
    try testing.expect(registry.counters.contains("compaction_expired_prepared_total"));
    try testing.expect(registry.counters.contains("compaction_errors_total"));
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

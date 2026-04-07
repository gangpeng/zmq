/// AutoMQ Broker
///
/// Full broker implementation:
/// - Broker: stateful request handler for all Kafka APIs
/// - PartitionStore: topic-partition storage with WAL + cache + S3
/// - GroupCoordinator: consumer group lifecycle
/// - TransactionCoordinator: exactly-once transaction support
/// - QuotaManager: client rate limiting and throttling
/// - Purgatory: delayed operations with timer wheel
/// - Metrics: Prometheus-compatible metric registry
/// - Persistence: topic and offset metadata persistence
/// - FetchSessions: KIP-227 incremental fetch sessions

pub const handler = @import("broker/handler.zig");
pub const partition_store = @import("broker/partition_store.zig");
pub const group_coordinator = @import("broker/group_coordinator.zig");
pub const txn_coordinator = @import("broker/txn_coordinator.zig");
pub const quota_manager = @import("broker/quota_manager.zig");
pub const purgatory = @import("broker/purgatory.zig");
pub const metrics = @import("broker/metrics.zig");
pub const persistence = @import("broker/persistence.zig");
pub const fetch_session = @import("broker/fetch_session.zig");
pub const response_builder = @import("broker/response_builder.zig");
// Principle 4 fix: Broker failure handling
pub const failover = @import("broker/failover.zig");
// Principle 5 fix: Load balancing & partition reassignment
pub const auto_balancer = @import("broker/auto_balancer.zig");

pub const Broker = handler.Broker;
pub const handleRequest = handler.handleRequest;
pub const PartitionStore = partition_store.PartitionStore;
pub const GroupCoordinator = group_coordinator.GroupCoordinator;
pub const TransactionCoordinator = txn_coordinator.TransactionCoordinator;
pub const QuotaManager = quota_manager.QuotaManager;
pub const TimerWheel = purgatory.TimerWheel;
pub const MetricRegistry = metrics.MetricRegistry;
pub const MetadataPersistence = persistence.MetadataPersistence;
pub const FetchSessionManager = fetch_session.FetchSessionManager;
pub const ResponseBuilder = response_builder.ResponseBuilder;
// Principle 4 fix: Failover controller for broker failure handling
pub const FailoverController = failover.FailoverController;
// Principle 5 fix: Auto-balancer for partition reassignment
pub const AutoBalancer = auto_balancer.AutoBalancer;

test {
    @import("std").testing.refAllDecls(@This());
}

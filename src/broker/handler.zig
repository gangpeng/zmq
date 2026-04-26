const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.broker);

const protocol = @import("protocol");
const ser = protocol.serialization;
const header_mod = protocol.header;
const api_support = protocol.api_support;
const generated = protocol.generated;
const ErrorCode = protocol.ErrorCode;
const api_versions_mod = protocol.messages.api_versions;
const delete_groups_mod = protocol.generated.delete_groups_response;
const RequestHeader = header_mod.RequestHeader;
const ResponseHeader = header_mod.ResponseHeader;
const ApiVersionsResponse = api_versions_mod.ApiVersionsResponse;
const DeleteGroupsResponse = delete_groups_mod.DeleteGroupsResponse;
const PartitionStore = @import("partition_store.zig").PartitionStore;
const GroupCoordinator = @import("group_coordinator.zig").GroupCoordinator;
const TxnCoordinator = @import("txn_coordinator.zig").TransactionCoordinator;
const QuotaManager = @import("quota_manager.zig").QuotaManager;
const MetricRegistry = @import("metrics.zig").MetricRegistry;
const metrics_mod = @import("metrics.zig");
const MetadataPersistence = @import("persistence.zig").MetadataPersistence;
const RaftState = @import("raft").RaftState;
const auth_mod = @import("security").auth;
const Authorizer = auth_mod.Authorizer;
const SaslPlainAuthenticator = auth_mod.SaslPlainAuthenticator;
const OAuthBearerAuthenticator = auth_mod.OAuthBearerAuthenticator;
const ScramSha256Authenticator = auth_mod.ScramSha256Authenticator;
const ScramStateMachine = auth_mod.ScramStateMachine;
const FetchSessionManager = @import("fetch_session.zig").FetchSessionManager;
// Import failover controller
const FailoverController = @import("failover.zig").FailoverController;
// Import auto balancer
const AutoBalancer = @import("auto_balancer.zig").AutoBalancer;
// Stream/StreamSetObject/StreamObject metadata + compaction
const storage = @import("storage");
const ObjectManager = storage.ObjectManager;
const CompactionManager = storage.CompactionManager;
const JsonLogger = @import("core").JsonLogger;

/// Stateful Kafka broker.
///
/// Owns all broker-level state: partition storage, consumer group
/// coordination, transactions, quotas, metrics.
pub const Broker = struct {
    // No global mutex needed — the broker runs in a single-threaded
    // epoll/io_uring event loop with inline handler calls. Removing the mutex
    // eliminates unnecessary synchronization overhead.
    // If multi-threaded processing is added later, use per-partition locks instead.
    store: PartitionStore,
    groups: GroupCoordinator,
    txn_coordinator: TxnCoordinator,
    quota_manager: QuotaManager,
    metrics: MetricRegistry,
    persistence: MetadataPersistence,
    /// Map of topic name → topic metadata (partitions, replication factor, config).
    topics: std.StringHashMap(TopicInfo),
    /// AutoMQ controller KV namespace. Values are owned by this map.
    auto_mq_kvs: std.StringHashMap([]u8),
    /// AutoMQ node registry keyed by node_id.
    auto_mq_nodes: std.AutoHashMap(i32, AutoMqNodeMetadata),
    /// Next node ID returned by AutoMQ GetNextNodeId.
    auto_mq_next_node_id: i32 = 1,
    /// Last license payload supplied through AutoMQ UpdateLicense.
    auto_mq_license: ?[]u8 = null,
    /// Last zone router metadata blob supplied by clients.
    auto_mq_zone_router_metadata: ?[]u8 = null,
    /// Latest zone router epoch accepted by the broker.
    auto_mq_zone_router_epoch: i64 = 0,
    /// AutoMQ link/group promotion state keyed by group_id.
    auto_mq_group_promotions: std.StringHashMap(AutoMqGroupPromotion),
    /// Idempotent producer deduplication: tracks last sequence number per producer+partition.
    producer_sequences: std.AutoHashMap(ProducerKey, ProducerSequenceState),
    allocator: Allocator,
    /// KRaft node ID for this broker in the cluster.
    node_id: i32,
    /// Kafka protocol listener port.
    port: u16,
    /// Whether to automatically create topics on first produce/fetch.
    auto_create_topics: bool,
    /// Default number of partitions for auto-created topics.
    default_num_partitions: i32,
    /// Default replication factor for auto-created topics.
    default_replication_factor: i16,
    /// Hostname advertised to clients in metadata responses.
    advertised_host: []const u8,
    /// Raft consensus state. Non-null when this node has the controller role.
    /// Null in broker-only mode (metadata comes from the controller via MetadataClient).
    raft_state: ?*RaftState = null,
    /// Cached leader epoch received from the controller (used in broker-only mode).
    cached_leader_epoch: i32 = 0,
    /// Set to true when the controller fences this broker (e.g., heartbeat timeout).
    /// When fenced, the broker rejects all produce requests with NOT_LEADER_OR_FOLLOWER.
    is_fenced_by_controller: bool = false,
    /// Set to true when the broker is shutting down gracefully.
    /// When set, new produce/fetch requests are rejected with NOT_LEADER_OR_FOLLOWER (error 6),
    /// which tells clients to refresh metadata and reconnect to another broker.
    /// ApiVersions (key 18) is still allowed so clients can probe connectivity.
    is_shutting_down: bool = false,
    /// Timestamp (ms) of the last successful controller heartbeat.
    /// Used for staleness detection: if too old, the broker self-fences.
    last_successful_heartbeat_ms: i64 = 0,
    authorizer: Authorizer,
    /// SASL/PLAIN authenticator for client authentication.
    sasl_authenticator: SaslPlainAuthenticator,
    /// SCRAM-SHA-256 authenticator for challenge-response authentication.
    scram_authenticator: ScramSha256Authenticator,
    /// OAUTHBEARER authenticator for JWT-based authentication.
    oauth_authenticator: OAuthBearerAuthenticator,
    /// Whether SASL authentication is required for client connections.
    sasl_enabled: bool = false,
    /// Map of client_id → authenticated principal (from SASL handshake).
    /// Used to resolve the real principal for ACL checks instead of trusting
    /// the unauthenticated client_id header.
    authenticated_sessions: std.StringHashMap([]u8),
    /// Map of client_id → negotiated SASL mechanism name.
    /// Tracks which mechanism each client selected during SaslHandshake.
    sasl_mechanisms: std.StringHashMap([]u8),
    /// Comma-separated list of enabled SASL mechanisms (e.g., "PLAIN,SCRAM-SHA-256,OAUTHBEARER").
    sasl_enabled_mechanisms: []const u8 = "PLAIN",
    /// Per-connection SCRAM-SHA-256 state machines for multi-round authentication.
    /// Maps client_id → ScramStateMachine tracking the exchange progress.
    scram_sessions: std.StringHashMap(ScramStateMachine),
    /// KIP-227 incremental fetch session manager.
    fetch_sessions: FetchSessionManager,
    /// Counter for S3 flush failures (used for monitoring/alerting).
    s3_flush_failures: u64 = 0,
    /// Dirty flag for producer sequence persistence.
    producer_sequences_dirty: bool = false,
    /// Timestamp of last retention enforcement run.
    /// Retention runs every 60 seconds (matching AutoMQ's LogCleaner interval).
    last_retention_check_ms: i64 = 0,
    /// Failover controller for broker failure handling.
    failover_controller: FailoverController,
    /// Auto-balancer for partition reassignment.
    auto_balancer: AutoBalancer,
    /// Stream/StreamSetObject/StreamObject metadata registry + S3 object resolution.
    object_manager: ObjectManager,
    /// Compaction manager: splits multi-stream SSOs into per-stream SOs, merges small SOs.
    compaction_manager: ?CompactionManager = null,
    /// Delayed fetch purgatory — stores fetch requests waiting for new data.
    /// When a fetch returns empty and max_wait_ms > 0, the request is stored here
    /// and completed when new data arrives or the timeout expires.
    delayed_fetches: std.array_list.Managed(DelayedFetch) = undefined,
    /// Structured JSON logger for production-critical log statements.
    json_logger: JsonLogger = undefined,

    /// Delayed fetch entry for purgatory.
    pub const DelayedFetch = struct {
        /// Kafka protocol correlation ID for matching response to request.
        correlation_id: i32,
        topic: []u8,
        partition_id: i32,
        /// Starting offset the client is fetching from.
        fetch_offset: u64,
        max_bytes: usize,
        /// Wall-clock deadline (ms) after which the fetch expires without data.
        deadline_ms: i64,
        /// Kafka API version of the original fetch request.
        api_version: i16,
        /// Response header version matching the request's flexible version.
        resp_header_version: i16,
        /// Pre-serialized response header bytes needed to build the response
        client_id: ?[]u8,

        pub fn deinit(self: *DelayedFetch, alloc: Allocator) void {
            alloc.free(self.topic);
            if (self.client_id) |cid| alloc.free(cid);
        }

        pub fn isExpired(self: *const DelayedFetch) bool {
            return @import("time_compat").milliTimestamp() >= self.deadline_ms;
        }
    };

    pub const TopicInfo = struct {
        name: []u8,
        num_partitions: i32,
        replication_factor: i16,
        /// UUID v4 identifying this topic (all zeros until assigned).
        topic_id: [16]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        /// Per-topic configuration overrides (retention, max message size, etc.).
        config: TopicConfig = .{},

        /// Generate a random topic UUID (v4 UUID).
        pub fn generateTopicId() [16]u8 {
            var uuid: [16]u8 = undefined;
            @import("random_compat").bytes(&uuid);
            // Set version 4 (random) and variant bits
            uuid[6] = (uuid[6] & 0x0f) | 0x40; // version 4
            uuid[8] = (uuid[8] & 0x3f) | 0x80; // variant 1
            return uuid;
        }
    };

    pub const AutoMqNodeMetadata = struct {
        node_epoch: i64,
        wal_config: []u8,
        state: []const u8 = "ACTIVE",

        pub fn deinit(self: *AutoMqNodeMetadata, alloc: Allocator) void {
            alloc.free(self.wal_config);
        }
    };

    pub const AutoMqGroupPromotion = struct {
        link_id: []u8,
        promoted: bool,

        pub fn deinit(self: *AutoMqGroupPromotion, alloc: Allocator) void {
            alloc.free(self.link_id);
        }
    };

    /// Per-topic configuration (mirrors Kafka topic configs).
    pub const TopicConfig = struct {
        retention_ms: i64 = 604800000, // 7 days
        retention_bytes: i64 = -1, // unlimited
        max_message_bytes: i32 = 1048576, // 1MB
        segment_bytes: i64 = 1073741824, // 1GB
        cleanup_policy: []const u8 = "delete",
        compression_type: []const u8 = "producer",
        min_insync_replicas: i32 = 1,
        flush_messages: i64 = 9223372036854775807, // Long.MAX
        flush_ms: i64 = 9223372036854775807,
        max_compaction_lag_ms: i64 = 9223372036854775807,
        min_compaction_lag_ms: i64 = 0,
        unclean_leader_election_enable: bool = false,
    };

    /// Key for idempotent producer sequence tracking.
    pub const ProducerKey = struct {
        producer_id: i64,
        /// Hash of "topic-partition" string to uniquely identify the target partition.
        partition_key: u64,
    };

    /// Tracks producer idempotency state per (producer_id, partition).
    /// NOTE: AutoMQ/Kafka uses ProducerStateManager per partition to track
    /// sequence numbers, epochs, and offsets for idempotent deduplication.
    pub const ProducerSequenceState = struct {
        last_sequence: i32,
        producer_epoch: i16,
    };

    pub const BrokerConfig = struct {
        data_dir: ?[]const u8 = null,
        s3_endpoint_host: ?[]const u8 = null,
        s3_endpoint_port: u16 = 9000,
        s3_bucket: []const u8 = "automq",
        s3_access_key: []const u8 = "minioadmin",
        s3_secret_key: []const u8 = "minioadmin",
        s3_tls_ca_file: ?[]const u8 = null,
        auto_create_topics: bool = true,
        default_num_partitions: i32 = 1,
        default_replication_factor: i16 = 1,
        advertised_host: []const u8 = "localhost",
        /// message.max.bytes — maximum record batch size (default 1MB)
        message_max_bytes: i32 = 1048576,
        /// log.retention.ms — retention time in ms (default 7 days, -1 = unlimited)
        log_retention_ms: i64 = 604800000,
        /// log.retention.bytes — retention size per partition (-1 = unlimited)
        log_retention_bytes: i64 = -1,
        /// compression.type — broker-level compression ("producer" = passthrough)
        compression_type: []const u8 = "producer",

        // S3 WAL performance configuration
        /// S3 WAL batch size in bytes (default 4MB)
        s3_wal_batch_size: usize = 4 * 1024 * 1024,
        /// S3 WAL flush interval in milliseconds (default 250ms)
        s3_wal_flush_interval_ms: i64 = 250,
        /// S3 WAL flush mode: sync = flush after every produce (durable),
        /// async = batch flush (fast but lossy). Default: sync for correctness.
        s3_wal_flush_mode: WalFlushMode = .sync,

        // Cache configuration
        /// Maximum number of blocks in the log cache (default 64)
        cache_max_blocks: usize = 64,
        /// Maximum log cache size in bytes (default 256MB)
        cache_max_size: u64 = 256 * 1024 * 1024,
        /// S3 block cache size in bytes (default 64MB)
        s3_block_cache_size: u64 = 64 * 1024 * 1024,

        // Network configuration
        /// Number of network/IO threads (default 3)
        network_threads: usize = 3,
        /// Number of IO processing threads (default 8)
        io_threads: usize = 8,

        // Compaction configuration
        /// Log compaction interval in milliseconds (default 5 minutes)
        compaction_interval_ms: i64 = 300_000,

        // Security configuration
        /// Enable SASL authentication (default: false for backward compatibility)
        sasl_enabled: bool = false,
        /// Comma-separated user:password pairs for SASL/PLAIN (e.g., "admin:secret,user1:pass1")
        sasl_users: []const u8 = "",
        /// Semicolon-separated principals that bypass ACLs (e.g., "User:admin;User:broker")
        super_users: []const u8 = "",
        /// Allow all operations when no ACLs are defined (default: true)
        allow_everyone_if_no_acl: bool = true,

        /// Comma-separated SASL mechanisms to enable (e.g., "PLAIN,SCRAM-SHA-256,OAUTHBEARER")
        sasl_enabled_mechanisms: []const u8 = "PLAIN",
        /// Expected issuer for OAUTHBEARER tokens (empty = no validation)
        oauth_issuer: []const u8 = "",
        /// Expected audience for OAUTHBEARER tokens (empty = no validation)
        oauth_audience: []const u8 = "",

        // TLS configuration (used by main.zig to create TlsConfig)
        /// Security protocol: "plaintext", "ssl", "sasl_plaintext", "sasl_ssl"
        security_protocol: []const u8 = "plaintext",
        /// Path to PEM-encoded server certificate chain
        tls_cert_file: ?[]const u8 = null,
        /// Path to PEM-encoded server private key
        tls_key_file: ?[]const u8 = null,
        /// Path to PEM-encoded CA certificate(s) for client verification (mTLS)
        tls_ca_file: ?[]const u8 = null,
        /// Client certificate auth mode: "none", "requested", "required"
        tls_client_auth: []const u8 = "none",
    };

    pub const WalFlushMode = storage.wal.WalFlushMode;

    fn defaultAutoMqNextNodeId(node_id: i32) i32 {
        if (node_id == std.math.maxInt(i32)) return std.math.maxInt(i32);
        return @max(node_id + 1, 1);
    }

    pub fn init(alloc: Allocator, node_id: i32, port: u16) Broker {
        return initWithConfig(alloc, node_id, port, .{});
    }

    pub fn initWithConfig(alloc: Allocator, node_id: i32, port: u16, config: BrokerConfig) Broker {
        // Enable S3 WAL mode when S3 endpoint is configured
        const s3_configured = config.s3_endpoint_host != null;
        var broker = Broker{
            .store = PartitionStore.initWithConfig(alloc, .{
                .data_dir = config.data_dir,
                .s3_endpoint_host = config.s3_endpoint_host,
                .s3_endpoint_port = config.s3_endpoint_port,
                .s3_bucket = config.s3_bucket,
                .s3_access_key = config.s3_access_key,
                .s3_secret_key = config.s3_secret_key,
                .s3_tls_ca_file = config.s3_tls_ca_file,
                // Pass through configurable cache parameters
                .cache_max_blocks = config.cache_max_blocks,
                .cache_max_size = config.cache_max_size,
                // Enable S3 WAL mode for batched, durable S3 writes
                .s3_wal_mode = s3_configured,
                .s3_wal_batch_size = config.s3_wal_batch_size,
                .s3_wal_flush_interval_ms = config.s3_wal_flush_interval_ms,
                .s3_wal_flush_mode = config.s3_wal_flush_mode,
            }),
            .groups = GroupCoordinator.init(alloc),
            .txn_coordinator = TxnCoordinator.init(alloc),
            .quota_manager = QuotaManager.init(alloc),
            .metrics = MetricRegistry.init(alloc),
            .persistence = MetadataPersistence.init(alloc, config.data_dir),
            .topics = std.StringHashMap(TopicInfo).init(alloc),
            .auto_mq_kvs = std.StringHashMap([]u8).init(alloc),
            .auto_mq_nodes = std.AutoHashMap(i32, AutoMqNodeMetadata).init(alloc),
            .auto_mq_next_node_id = defaultAutoMqNextNodeId(node_id),
            .auto_mq_group_promotions = std.StringHashMap(AutoMqGroupPromotion).init(alloc),
            .producer_sequences = std.AutoHashMap(ProducerKey, ProducerSequenceState).init(alloc),
            .allocator = alloc,
            .node_id = node_id,
            .port = port,
            .auto_create_topics = config.auto_create_topics,
            .default_num_partitions = config.default_num_partitions,
            .default_replication_factor = config.default_replication_factor,
            .advertised_host = config.advertised_host,
            .authorizer = Authorizer.init(alloc),
            .sasl_authenticator = SaslPlainAuthenticator.init(alloc),
            .scram_authenticator = ScramSha256Authenticator.init(alloc),
            .oauth_authenticator = OAuthBearerAuthenticator.init(),
            .authenticated_sessions = std.StringHashMap([]u8).init(alloc),
            .sasl_mechanisms = std.StringHashMap([]u8).init(alloc),
            .scram_sessions = std.StringHashMap(ScramStateMachine).init(alloc),
            .fetch_sessions = FetchSessionManager.init(alloc),
            // Initialize failover controller
            .failover_controller = FailoverController.init(alloc, node_id),
            // Initialize auto-balancer
            .auto_balancer = AutoBalancer.init(alloc),
            // Initialize ObjectManager for Stream/StreamSetObject/StreamObject tracking
            .object_manager = ObjectManager.init(alloc, node_id),
        };

        // Initialize delayed fetch purgatory
        broker.delayed_fetches = std.array_list.Managed(DelayedFetch).init(alloc);

        // Initialize structured JSON logger
        broker.json_logger = JsonLogger.init(alloc);

        // ObjectManager is wired after the Broker reaches its final address.
        // Setting self-references here would leave dangling pointers when the
        // Broker value is returned from this function.

        // Initialize CompactionManager if S3 storage is configured
        if (broker.store.s3_storage != null) {
            broker.compaction_manager = CompactionManager.init(alloc, &broker.object_manager);
            if (broker.compaction_manager) |*cm| {
                cm.compaction_interval_ms = config.compaction_interval_ms;
                cm.s3_storage = &broker.store.s3_storage.?;
            }
        }

        // Register standard broker metrics (does not take &self pointers,
        // only registers names in the MetricRegistry's hash maps)
        metrics_mod.registerBrokerMetrics(&broker.metrics) catch {};
        metrics_mod.registerS3Metrics(&broker.metrics) catch {};
        metrics_mod.registerCompactionMetrics(&broker.metrics) catch {};
        metrics_mod.registerCacheMetrics(&broker.metrics) catch {};
        metrics_mod.registerRaftMetrics(&broker.metrics) catch {};

        // NOTE: Do NOT wire &broker.metrics into subsystems here!
        // This function returns a Broker by value which gets moved to its
        // final heap location via alloc.create(). Any &broker.X pointers
        // captured here would become dangling. Call wireInternalPointers()
        // after the Broker is at its final address.

        // Configure security settings
        broker.sasl_enabled = config.sasl_enabled;
        broker.authorizer.allow_everyone_if_no_acl = config.allow_everyone_if_no_acl;

        // Parse sasl.users: "user1:pass1,user2:pass2"
        if (config.sasl_users.len > 0) {
            var user_pairs = std.mem.splitSequence(u8, config.sasl_users, ",");
            while (user_pairs.next()) |pair| {
                if (std.mem.indexOf(u8, pair, ":")) |colon| {
                    const username = pair[0..colon];
                    const password = pair[colon + 1 ..];
                    broker.sasl_authenticator.addUser(username, password) catch {};
                }
            }
        }

        // Configure multi-mechanism SASL settings
        broker.sasl_enabled_mechanisms = config.sasl_enabled_mechanisms;
        if (config.oauth_issuer.len > 0 or config.oauth_audience.len > 0) {
            broker.oauth_authenticator = OAuthBearerAuthenticator.initWithConfig(
                if (config.oauth_issuer.len > 0) config.oauth_issuer else null,
                if (config.oauth_audience.len > 0) config.oauth_audience else null,
            );
        }

        // Parse super.users: "User:admin;User:broker"
        if (config.super_users.len > 0) {
            var su_iter = std.mem.splitSequence(u8, config.super_users, ";");
            while (su_iter.next()) |su| {
                if (su.len > 0) {
                    broker.authorizer.addSuperUser(su) catch {};
                }
            }
        }

        return broker;
    }

    /// Wire in the Raft state (owned externally by Controller or main).
    pub fn setRaftState(self: *Broker, raft: *RaftState) void {
        self.raft_state = raft;
        // Wire metrics into the Raft state for consensus observability
        raft.metrics = &self.metrics;
    }

    /// Establish internal self-referencing pointers after the Broker struct
    /// is at its final heap address. Must be called once after alloc.create(Broker)
    /// + assignment.
    ///
    /// During initWithConfig(), the Broker is constructed as a stack value and
    /// then moved to the heap via `brk.* = Broker.initWithConfig(...)`. Any
    /// pointers from subsystems to &broker.metrics (or other Broker fields)
    /// captured during init would become dangling after the move. This method
    /// re-wires them to point at the final heap address.
    pub fn wireInternalPointers(self: *Broker) void {
        self.store.setObjectManager(&self.object_manager);

        // Wire metrics registry into subsystems
        self.store.cache.metrics = &self.metrics;
        if (self.store.s3_client) |*c| {
            c.metrics = &self.metrics;
        }
        if (self.store.s3_block_cache) |*bc| {
            bc.metrics = &self.metrics;
        }
        if (self.compaction_manager) |*cm| {
            cm.object_manager = &self.object_manager;
            if (self.store.s3_storage) |*storage_ref| {
                cm.s3_storage = storage_ref;
            }
            cm.metrics = &self.metrics;
        }
        // Wire metrics into group coordinator for consumer lag tracking
        self.groups.metrics = &self.metrics;
    }

    /// Open the broker (initializes storage, WAL, loads persisted metadata)
    pub fn open(self: *Broker) !void {
        self.wireInternalPointers();
        try self.store.open();
        const restored_object_snapshot = try self.restoreObjectManagerSnapshot();
        if (!restored_object_snapshot) {
            const recovered_s3_objects = try self.store.recoverS3WalObjects();
            if (recovered_s3_objects > 0) {
                log.info("Rebuilt {d} S3 WAL object(s) into ObjectManager", .{recovered_s3_objects});
                self.persistObjectManagerSnapshot();
            }
        }

        // Load persisted topics
        const saved_topics = try self.persistence.loadTopics();
        defer {
            for (saved_topics) |entry| self.allocator.free(entry.name);
            self.allocator.free(saved_topics);
        }

        for (saved_topics) |entry| {
            const name_copy = try self.allocator.dupe(u8, entry.name);
            errdefer self.allocator.free(name_copy);
            const key_copy = try self.allocator.dupe(u8, entry.name);
            errdefer self.allocator.free(key_copy);

            try self.topics.put(key_copy, .{
                .name = name_copy,
                .num_partitions = entry.num_partitions,
                .replication_factor = entry.replication_factor,
                .topic_id = TopicInfo.generateTopicId(),
            });

            // Ensure partitions exist in storage
            for (0..@as(usize, @intCast(entry.num_partitions))) |pi| {
                self.store.ensurePartition(entry.name, @intCast(pi)) catch |err| {
                    log.warn("Failed to ensure partition {s}-{d}: {}", .{ entry.name, pi, err });
                };
            }

            log.info("Loaded persisted topic '{s}' ({d} partitions)", .{ entry.name, entry.num_partitions });
        }

        // Ensure internal topics exist
        try self.ensureInternalTopic("__consumer_offsets", 50);
        try self.ensureInternalTopic("__transaction_state", 50);

        // Load persisted offsets into group coordinator
        const saved_offsets = try self.persistence.loadOffsets();
        defer {
            for (saved_offsets) |entry| self.allocator.free(entry.key);
            self.allocator.free(saved_offsets);
        }

        for (saved_offsets) |entry| {
            // Key format: "group:topic:partition" (as stored by GroupCoordinator.commitOffset)
            var parts = std.mem.splitSequence(u8, entry.key, ":");
            const group_id = parts.next() orelse continue;
            const topic = parts.next() orelse continue;
            const partition_str = parts.next() orelse continue;
            const partition = std.fmt.parseInt(i32, partition_str, 10) catch continue;

            self.groups.commitOffset(group_id, topic, partition, entry.offset) catch |err| {
                log.warn("Failed to restore offset for {s}/{s}-{d}: {}", .{ group_id, topic, partition, err });
            };
        }

        // Load persisted transaction state
        const txn_snapshot = try self.persistence.loadTransactions();
        defer {
            for (txn_snapshot.entries) |e| {
                if (e.transactional_id) |tid| self.allocator.free(tid);
            }
            self.allocator.free(txn_snapshot.entries);
        }
        if (txn_snapshot.entries.len > 0 or txn_snapshot.next_producer_id > 1000) {
            try self.txn_coordinator.restoreState(txn_snapshot);
            log.info("Restored {d} transaction(s), next_producer_id={d}", .{ txn_snapshot.entries.len, txn_snapshot.next_producer_id });
        }

        // Load persisted producer sequences
        const saved_sequences = try self.persistence.loadProducerSequences();
        defer self.allocator.free(saved_sequences);
        for (saved_sequences) |entry| {
            self.producer_sequences.put(.{
                .producer_id = entry.producer_id,
                .partition_key = entry.partition_key,
            }, .{
                .last_sequence = entry.last_sequence,
                .producer_epoch = entry.producer_epoch,
            }) catch |err| {
                log.warn("Failed to restore producer sequence pid={d}: {}", .{ entry.producer_id, err });
            };
        }
        if (saved_sequences.len > 0) {
            log.info("Restored {d} producer sequence state(s)", .{saved_sequences.len});
        }

        // Load persisted ACLs
        const saved_acls = try self.persistence.loadAcls();
        defer {
            for (saved_acls) |entry| {
                self.allocator.free(entry.principal);
                self.allocator.free(entry.resource_name);
                self.allocator.free(entry.host);
            }
            self.allocator.free(saved_acls);
        }
        for (saved_acls) |entry| {
            self.authorizer.addAcl(
                entry.principal,
                @enumFromInt(entry.resource_type),
                entry.resource_name,
                @enumFromInt(entry.pattern_type),
                @enumFromInt(entry.operation),
                @enumFromInt(entry.permission),
                entry.host,
            ) catch |err| {
                log.warn("Failed to restore ACL: {}", .{err});
            };
        }
        if (saved_acls.len > 0) {
            log.info("Restored {d} ACL(s) from acls.meta", .{saved_acls.len});
        }

        // Load persisted partition offsets and visibility state after topics and
        // internal partitions exist.
        const saved_partition_states = try self.persistence.loadPartitionStates();
        defer {
            for (saved_partition_states) |entry| self.allocator.free(entry.topic);
            self.allocator.free(saved_partition_states);
        }
        try self.restorePartitionStates(saved_partition_states);
        if (saved_partition_states.len > 0) {
            log.info("Restored {d} partition state(s) from partition_state.meta", .{saved_partition_states.len});
        }
        if (self.store.repairPartitionStatesFromObjectManager()) {
            log.info("Repaired partition state from recovered ObjectManager streams", .{});
            self.persistPartitionStates();
        }

        // Load local AutoMQ controller-style metadata (KV namespace, node registry,
        // license, zone router state, and group promotions).
        var auto_mq_snapshot = try self.persistence.loadAutoMqMetadata();
        defer self.persistence.freeAutoMqMetadataSnapshot(&auto_mq_snapshot);
        try self.restoreAutoMqMetadata(auto_mq_snapshot);
        if (auto_mq_snapshot.kvs.len > 0 or auto_mq_snapshot.nodes.len > 0 or auto_mq_snapshot.group_promotions.len > 0 or auto_mq_snapshot.license != null or auto_mq_snapshot.zone_router_metadata != null) {
            log.info("Restored AutoMQ metadata (kvs={d}, nodes={d}, groups={d}, next_node_id={d})", .{
                auto_mq_snapshot.kvs.len,
                auto_mq_snapshot.nodes.len,
                auto_mq_snapshot.group_promotions.len,
                self.auto_mq_next_node_id,
            });
        }
    }

    /// Periodic maintenance — should be called every ~1 second.
    /// Handles: session timeout eviction, rebalance timeouts, S3 flush, metrics.
    pub fn tick(self: *Broker) void {
        // Evict expired consumer group members (30 second timeout)
        const evicted = self.groups.evictExpiredMembers(30000);
        if (evicted > 0) {
            log.info("Evicted {d} expired group members", .{evicted});
        }

        // Check for rebalance timeouts
        const forced = self.groups.checkRebalanceTimeouts();
        if (forced > 0) {
            log.info("Force-completed {d} timed-out rebalances", .{forced});
        }

        // Expire timed-out transactions (auto-abort after timeout_ms).
        // NOTE: AutoMQ/Kafka runs this check periodically to prevent resource leaks
        // from abandoned producers that never call EndTxn.
        const txn_expired = self.txn_coordinator.expireTransactions();
        if (txn_expired > 0) {
            log.info("Auto-aborted {d} timed-out transaction(s)", .{txn_expired});
        }

        // Persist transaction state if dirty
        if (self.txn_coordinator.dirty) {
            self.persistence.saveTransactions(&self.txn_coordinator) catch |err| {
                log.warn("Failed to persist transaction state: {}", .{err});
            };
            self.txn_coordinator.dirty = false;
        }

        // Persist producer sequences if dirty
        if (self.producer_sequences_dirty) {
            self.persistence.saveProducerSequences(&self.producer_sequences) catch |err| {
                log.warn("Failed to persist producer sequences: {}", .{err});
            };
            self.producer_sequences_dirty = false;
        }

        // Periodic S3 flush (if configured)
        if (self.store.s3_storage != null) {
            self.store.flushAllToS3() catch |err| {
                log.warn("S3 flush failed, will retry: {}", .{err});
                self.json_logger.log(.warn, "S3 flush failed, will retry", null);
                self.s3_flush_failures += 1;
            };
        }

        // Flush S3 WAL batcher if threshold reached
        if (self.store.s3_wal_batcher) |*batcher| {
            if (batcher.shouldFlush()) {
                if (self.store.s3_storage) |*s3| {
                    if (batcher.flushNow(s3)) {
                        self.persistPartitionStates();
                        self.persistObjectManagerSnapshot();
                    }
                }
            }
        }

        // Clean up WAL segments that have been durably flushed to S3.
        // NOTE: AutoMQ trims WAL after S3 upload confirmation in
        // S3Storage.commitStreamSetObject(). ZMQ does it in tick() since
        // the single-threaded model makes WAL cleanup non-urgent.
        if (self.store.s3_wal_batcher) |*batcher| {
            if (batcher.last_flushed_segment_id > 0) {
                if (self.store.fs_wal) |*wal| {
                    _ = wal.cleanupSegments(batcher.last_flushed_segment_id - 1) catch |err| {
                        log.warn("WAL segment cleanup failed: {}", .{err});
                    };
                }
            }
        }

        // Expire delayed fetches
        self.expireDelayedFetches();

        // Check for failed nodes and trigger failover
        const failovers = self.failover_controller.tick(@import("time_compat").milliTimestamp());
        if (failovers > 0) {
            log.info("Failover: {d} nodes failed over", .{failovers});
            // Sync WAL epoch with failover controller
            if (self.store.s3_wal_batcher) |*batcher| {
                batcher.setEpoch(self.failover_controller.wal_epoch);
            }
        }

        // Track active group/member counts in metrics
        self.metrics.setGauge("kafka_server_group_count", @floatFromInt(self.groups.groupCount()));
        self.metrics.setGauge("kafka_server_topic_count", @floatFromInt(self.topics.count()));
        self.metrics.setGauge("kafka_server_member_count", @floatFromInt(self.groups.totalMemberCount()));

        // Periodically persist committed offsets and group state
        self.persistOffsets();

        // Enforce log retention policies every 60 seconds.
        // Advances stream.start_offset for partitions where data has exceeded
        // the configured retention_ms or retention_bytes. The actual S3 object
        // deletion happens in the compaction cycle below (cleanupExpired).
        //
        // NOTE: AutoMQ runs this in LogCleaner on a dedicated thread every 60s.
        // ZMQ runs it inline on the event loop since it only updates offsets
        // (no I/O), and compaction handles the actual S3 deletes.
        const now_ms = @import("time_compat").milliTimestamp();
        if (now_ms - self.last_retention_check_ms >= 60_000) {
            self.last_retention_check_ms = now_ms;
            self.enforceRetentionPolicies();
        }

        // Run S3 object compaction (split multi-stream SSOs, merge small SOs)
        if (self.compaction_manager) |*cm| {
            const before_compaction_ms = cm.last_compaction_ms;
            cm.maybeCompact();
            if (cm.last_compaction_ms != before_compaction_ms) {
                self.persistObjectManagerSnapshot();
            }
        }

        // Rotate dual-buffer prepared object registry if the 60-minute interval
        // has elapsed. This discards the oldest buffer and moves current → previous,
        // matching AutoMQ's ScheduledExecutorService-based rotation.
        self.object_manager.prepared_registry.maybeRotate();
    }

    /// Enforce retention policies for all topics.
    /// Iterates topics, builds a RetentionPolicy from each topic's config,
    /// and calls PartitionStore.applyRetention() to advance trim points.
    ///
    /// NOTE: AutoMQ's LogCleaner iterates all LogSegments and checks
    /// retention per-partition. ZMQ delegates to ObjectManager which
    /// checks StreamObject timestamps/sizes per-stream.
    fn enforceRetentionPolicies(self: *Broker) void {
        var total_trimmed: u64 = 0;

        var topic_it = self.topics.iterator();
        while (topic_it.next()) |entry| {
            const info = entry.value_ptr;
            const config = info.config;

            // Build retention policy from per-topic config
            const policy = PartitionStore.RetentionPolicy{
                .retention_ms = config.retention_ms,
                .retention_bytes = config.retention_bytes,
                .cleanup_policy = if (std.mem.eql(u8, config.cleanup_policy, "compact"))
                    .compact
                else if (std.mem.eql(u8, config.cleanup_policy, "compact,delete"))
                    .compact_delete
                else
                    .delete,
            };

            // Skip topics with unlimited retention and no size limit
            if (policy.retention_ms <= 0 and policy.retention_bytes <= 0) continue;
            if (policy.cleanup_policy != .delete and policy.cleanup_policy != .compact_delete) continue;

            const trimmed = self.store.applyRetention(policy) catch |err| {
                log.warn("Retention enforcement failed for topic {s}: {}", .{ entry.key_ptr.*, err });
                continue;
            };
            total_trimmed += trimmed;
        }

        if (total_trimmed > 0) {
            log.info("Retention enforcement: trimmed {d} partition(s)", .{total_trimmed});
            self.persistPartitionStates();
            self.persistObjectManagerSnapshot();
        }
    }

    /// Flush pending S3 WAL writes and advance HW for all affected partitions.
    /// Called from the Server's batch_flush_fn callback. The current produce
    /// path flushes group_commit writes before returning success, so this is
    /// primarily a safety net for future response-delayed batching.
    /// Returns true if flush succeeded (or nothing to flush).
    pub fn flushPendingWal(self: *Broker) bool {
        const batcher = &(self.store.s3_wal_batcher orelse return true);
        if (batcher.flush_mode != .group_commit) return true;
        if (!batcher.hasPendingFlush()) return true;

        // One S3 PUT for all accumulated produces in this batch
        if (self.store.s3_storage) |*s3| {
            const flushed = batcher.flushNow(s3);
            if (flushed) {
                // Apply deferred HW updates for all affected partitions
                var hw_updates = batcher.drainPendingHWUpdates();
                defer hw_updates.deinit();
                self.store.applyDeferredHWUpdates(&hw_updates);
                self.persistPartitionStates();
                self.persistObjectManagerSnapshot();
                return true;
            } else {
                log.warn("Group commit S3 flush failed ({d} pending produces)", .{batcher.pending_produce_count});
                self.s3_flush_failures += 1;
                // Data stays in batcher, HW not advanced — will retry on next iteration
                return false;
            }
        }
        return true;
    }

    /// Force flush all buffered WAL data to S3 during graceful shutdown.
    /// Unlike flushPendingWal() which respects batch thresholds, this flushes
    /// unconditionally to ensure no data is lost on shutdown.
    /// Returns true if flush succeeded (or nothing to flush).
    pub fn shutdownFlushWal(self: *Broker) bool {
        const batcher = &(self.store.s3_wal_batcher orelse return true);

        if (batcher.buffer.items.len == 0) return true;

        log.info("Shutdown: flushing {d} pending WAL entries to S3", .{batcher.buffer.items.len});

        if (self.store.s3_storage) |*s3| {
            const flushed = batcher.flushNow(s3);
            if (flushed) {
                var hw_updates = batcher.drainPendingHWUpdates();
                defer hw_updates.deinit();
                self.store.applyDeferredHWUpdates(&hw_updates);
                self.persistPartitionStates();
                self.persistObjectManagerSnapshot();
                log.info("Shutdown: WAL flush to S3 complete", .{});
                return true;
            } else {
                log.warn("Shutdown: WAL flush to S3 failed", .{});
                self.s3_flush_failures += 1;
                return false;
            }
        }
        return true;
    }

    pub fn deinit(self: *Broker) void {
        // Save state before shutdown
        self.persistTopics();
        self.persistOffsets();
        self.persistPartitionStates();
        self.persistAutoMqMetadata();
        self.persistObjectManagerSnapshot();

        var it = self.topics.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
            self.allocator.free(entry.key_ptr.*);
        }
        self.topics.deinit();
        self.clearAutoMqMetadata();
        self.auto_mq_kvs.deinit();
        self.auto_mq_nodes.deinit();
        self.auto_mq_group_promotions.deinit();
        self.producer_sequences.deinit();
        self.store.deinit();
        self.groups.deinit();
        self.txn_coordinator.deinit();
        self.quota_manager.deinit();
        self.metrics.deinit();
        self.authorizer.deinit();
        self.sasl_authenticator.deinit();
        self.scram_authenticator.deinit();
        // Free SASL mechanism negotiation map
        var mech_it = self.sasl_mechanisms.iterator();
        while (mech_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.sasl_mechanisms.deinit();
        // Free SCRAM session state machines
        var scram_it = self.scram_sessions.iterator();
        while (scram_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.scram_sessions.deinit();
        // Free authenticated session keys and values
        var sess_it = self.authenticated_sessions.iterator();
        while (sess_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.authenticated_sessions.deinit();
        self.fetch_sessions.deinit();
        // Clean up failover controller
        self.failover_controller.deinit();
        // Clean up auto-balancer
        self.auto_balancer.deinit();
        // Clean up ObjectManager (streams, SSOs, SOs)
        self.object_manager.deinit();
        // Clean up delayed fetches
        for (self.delayed_fetches.items) |*df| df.deinit(self.allocator);
        self.delayed_fetches.deinit();
        // Clean up structured JSON logger
        self.json_logger.deinit();
    }

    /// Persist current topic metadata to disk (best-effort).
    fn persistTopics(self: *Broker) void {
        self.persistence.saveTopics(&self.topics) catch |err| {
            log.warn("Failed to persist topics: {}", .{err});
        };
    }

    /// Persist ACLs to disk.
    fn persistAcls(self: *Broker) void {
        self.persistence.saveAcls(self.authorizer.acls.items) catch |err| {
            log.warn("Failed to persist ACLs: {}", .{err});
        };
    }

    /// Persist local AutoMQ controller-style metadata to disk (best-effort).
    fn persistAutoMqMetadata(self: *Broker) void {
        self.persistence.saveAutoMqMetadata(
            &self.auto_mq_kvs,
            &self.auto_mq_nodes,
            self.auto_mq_next_node_id,
            self.auto_mq_license,
            self.auto_mq_zone_router_metadata,
            self.auto_mq_zone_router_epoch,
            &self.auto_mq_group_promotions,
        ) catch |err| {
            log.warn("Failed to persist AutoMQ metadata: {}", .{err});
        };
    }

    /// Persist stream/object metadata snapshots to disk (best-effort).
    fn persistObjectManagerSnapshot(self: *Broker) void {
        var empty_orphans = [_][]const u8{};
        const orphaned_keys: []const []const u8 = if (self.compaction_manager) |*cm|
            cm.orphaned_keys.items
        else
            empty_orphans[0..];

        const object_snapshot = self.object_manager.takeSnapshot(orphaned_keys) catch |err| {
            log.warn("Failed to build ObjectManager snapshot: {}", .{err});
            return;
        };
        defer self.allocator.free(object_snapshot);

        self.persistence.saveObjectManagerSnapshot(object_snapshot) catch |err| {
            log.warn("Failed to persist ObjectManager snapshot: {}", .{err});
        };

        const prepared_snapshot = self.object_manager.prepared_registry.serialize(self.allocator) catch |err| {
            log.warn("Failed to build prepared object registry snapshot: {}", .{err});
            return;
        };
        defer self.allocator.free(prepared_snapshot);

        self.persistence.savePreparedObjectRegistrySnapshot(prepared_snapshot) catch |err| {
            log.warn("Failed to persist prepared object registry snapshot: {}", .{err});
        };
    }

    fn freeOrphanedKeys(self: *Broker, keys: [][]u8) void {
        for (keys) |key| self.allocator.free(key);
        self.allocator.free(keys);
    }

    fn attachOrphanedKeysToCompaction(self: *Broker, keys: [][]u8) !void {
        if (self.compaction_manager) |*cm| {
            var moved: usize = 0;
            while (moved < keys.len) : (moved += 1) {
                cm.orphaned_keys.append(keys[moved]) catch |err| {
                    var remaining = moved;
                    while (remaining < keys.len) : (remaining += 1) {
                        self.allocator.free(keys[remaining]);
                    }
                    self.allocator.free(keys);
                    return err;
                };
            }
            self.allocator.free(keys);
        } else {
            self.freeOrphanedKeys(keys);
        }
    }

    fn rebuildPreparedRegistryFromObjectSnapshot(self: *Broker) void {
        var so_it = self.object_manager.stream_objects.iterator();
        while (so_it.next()) |entry| {
            const so = entry.value_ptr;
            if (so.state == .prepared) {
                self.object_manager.prepared_registry.trackPreparedAt(so.object_id, so.state_changed_ms);
            }
        }

        var sso_it = self.object_manager.stream_set_objects.iterator();
        while (sso_it.next()) |entry| {
            const sso = entry.value_ptr;
            if (sso.state == .prepared) {
                self.object_manager.prepared_registry.trackPreparedAt(sso.object_id, sso.state_changed_ms);
            }
        }
    }

    fn restoreObjectManagerSnapshot(self: *Broker) !bool {
        const object_snapshot = try self.persistence.loadObjectManagerSnapshot() orelse {
            if (try self.persistence.loadPreparedObjectRegistrySnapshot()) |prepared_snapshot| {
                defer self.allocator.free(prepared_snapshot);
                try self.object_manager.prepared_registry.deserialize(prepared_snapshot);
            }
            return false;
        };
        defer self.allocator.free(object_snapshot);

        const orphaned_keys = try self.object_manager.loadSnapshot(object_snapshot);
        try self.attachOrphanedKeysToCompaction(orphaned_keys);

        if (try self.persistence.loadPreparedObjectRegistrySnapshot()) |prepared_snapshot| {
            defer self.allocator.free(prepared_snapshot);
            try self.object_manager.prepared_registry.deserialize(prepared_snapshot);
        } else {
            self.rebuildPreparedRegistryFromObjectSnapshot();
        }
        return true;
    }

    fn clearAutoMqMetadata(self: *Broker) void {
        var kv_it = self.auto_mq_kvs.iterator();
        while (kv_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.auto_mq_kvs.clearRetainingCapacity();

        var node_it = self.auto_mq_nodes.iterator();
        while (node_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.auto_mq_nodes.clearRetainingCapacity();

        if (self.auto_mq_license) |license| self.allocator.free(license);
        self.auto_mq_license = null;
        if (self.auto_mq_zone_router_metadata) |metadata| self.allocator.free(metadata);
        self.auto_mq_zone_router_metadata = null;
        self.auto_mq_zone_router_epoch = 0;
        self.auto_mq_next_node_id = defaultAutoMqNextNodeId(self.node_id);

        var group_promotion_it = self.auto_mq_group_promotions.iterator();
        while (group_promotion_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.auto_mq_group_promotions.clearRetainingCapacity();
    }

    fn restoreAutoMqMetadata(self: *Broker, snapshot: MetadataPersistence.AutoMqMetadataSnapshot) !void {
        self.clearAutoMqMetadata();
        errdefer self.clearAutoMqMetadata();
        self.auto_mq_next_node_id = @max(snapshot.next_node_id, defaultAutoMqNextNodeId(self.node_id));
        self.auto_mq_zone_router_epoch = snapshot.zone_router_epoch;

        if (snapshot.license) |license| {
            self.auto_mq_license = try self.allocator.dupe(u8, license);
        }
        if (snapshot.zone_router_metadata) |metadata| {
            self.auto_mq_zone_router_metadata = try self.allocator.dupe(u8, metadata);
        }

        for (snapshot.kvs) |entry| {
            const key_copy = try self.allocator.dupe(u8, entry.key);
            const value_copy = self.allocator.dupe(u8, entry.value) catch |err| {
                self.allocator.free(key_copy);
                return err;
            };
            self.auto_mq_kvs.put(key_copy, value_copy) catch |err| {
                self.allocator.free(key_copy);
                self.allocator.free(value_copy);
                return err;
            };
        }

        for (snapshot.nodes) |entry| {
            if (entry.node_id < 0) continue;
            const wal_config_copy = try self.allocator.dupe(u8, entry.wal_config);
            self.auto_mq_nodes.put(entry.node_id, .{
                .node_epoch = entry.node_epoch,
                .wal_config = wal_config_copy,
            }) catch |err| {
                self.allocator.free(wal_config_copy);
                return err;
            };
            if (entry.node_id < std.math.maxInt(i32) and entry.node_id >= self.auto_mq_next_node_id) {
                self.auto_mq_next_node_id = entry.node_id + 1;
            }
        }

        for (snapshot.group_promotions) |entry| {
            if (entry.group_id.len == 0) continue;
            const group_copy = try self.allocator.dupe(u8, entry.group_id);
            const link_copy = self.allocator.dupe(u8, entry.link_id) catch |err| {
                self.allocator.free(group_copy);
                return err;
            };
            self.auto_mq_group_promotions.put(group_copy, .{
                .link_id = link_copy,
                .promoted = entry.promoted,
            }) catch |err| {
                self.allocator.free(group_copy);
                self.allocator.free(link_copy);
                return err;
            };
        }
    }

    /// Auto-create a topic if auto.create.topics.enable is true.
    /// Returns true if topic existed or was created.
    fn ensureTopic(self: *Broker, topic_name: []const u8) bool {
        self.wireInternalPointers();

        if (self.topics.contains(topic_name)) return true;
        if (!self.auto_create_topics) return false;

        const name_copy = self.allocator.dupe(u8, topic_name) catch return false;
        const key_copy = self.allocator.dupe(u8, topic_name) catch {
            self.allocator.free(name_copy);
            return false;
        };

        self.topics.put(key_copy, .{
            .name = name_copy,
            .num_partitions = self.default_num_partitions,
            .replication_factor = self.default_replication_factor,
            .topic_id = TopicInfo.generateTopicId(),
        }) catch {
            self.allocator.free(name_copy);
            self.allocator.free(key_copy);
            return false;
        };

        // Create partition state
        const np: usize = @intCast(self.default_num_partitions);
        for (0..np) |pi| {
            self.store.ensurePartition(topic_name, @intCast(pi)) catch {};
        }

        log.info("Auto-created topic '{s}' with {d} partitions", .{ topic_name, self.default_num_partitions });
        self.persistTopics();
        self.persistObjectManagerSnapshot();
        return true;
    }

    /// Ensure an internal topic exists with the specified partition count.
    fn ensureInternalTopic(self: *Broker, name: []const u8, partitions: i32) !void {
        if (self.topics.contains(name)) return;

        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);
        const key_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key_copy);

        try self.topics.put(key_copy, .{
            .name = name_copy,
            .num_partitions = partitions,
            .replication_factor = 1,
            .topic_id = TopicInfo.generateTopicId(),
            .config = .{
                .cleanup_policy = "compact",
                .retention_ms = -1,
            },
        });

        for (0..@as(usize, @intCast(partitions))) |pi| {
            self.store.ensurePartition(name, @intCast(pi)) catch {};
        }

        log.info("Created internal topic '{s}' ({d} partitions, compact)", .{ name, partitions });
    }

    /// Persist committed offsets from group coordinator (best-effort).
    fn persistOffsets(self: *Broker) void {
        // The committed_offsets map has keys in format "group:topic:partition"
        // We save as-is using a wrapper that matches the persistence API
        var offsets_copy = std.StringHashMap(i64).init(self.allocator);
        defer {
            var kit = offsets_copy.keyIterator();
            while (kit.next()) |k| self.allocator.free(k.*);
            offsets_copy.deinit();
        }

        var it = self.groups.committed_offsets.iterator();
        while (it.next()) |entry| {
            const key = self.allocator.dupe(u8, entry.key_ptr.*) catch continue;
            offsets_copy.put(key, entry.value_ptr.*) catch {
                self.allocator.free(key);
            };
        }

        self.persistence.saveOffsets(&offsets_copy) catch |err| {
            log.warn("Failed to persist offsets: {}", .{err});
        };
    }

    /// Persist per-partition offsets/HW/LSO to disk (best-effort).
    fn persistPartitionStates(self: *Broker) void {
        self.persistence.savePartitionStates(&self.store.partitions) catch |err| {
            log.warn("Failed to persist partition states: {}", .{err});
        };
    }

    fn restorePartitionStates(self: *Broker, entries: []const MetadataPersistence.PartitionStateEntry) !void {
        for (entries) |entry| {
            if (entry.partition_id < 0) continue;
            if (!self.topics.contains(entry.topic)) continue;

            self.store.ensurePartition(entry.topic, entry.partition_id) catch |err| {
                log.warn("Failed to ensure partition for restored state {s}-{d}: {}", .{ entry.topic, entry.partition_id, err });
                continue;
            };

            const pkey = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ entry.topic, entry.partition_id }) catch continue;
            defer self.allocator.free(pkey);

            if (self.store.partitions.getPtr(pkey)) |state| {
                state.next_offset = entry.next_offset;
                state.log_start_offset = @min(entry.log_start_offset, state.next_offset);
                state.high_watermark = @min(@max(entry.high_watermark, state.log_start_offset), state.next_offset);
                state.first_unstable_txn_offset = if (entry.first_unstable_txn_offset) |unstable|
                    @min(@max(unstable, state.log_start_offset), state.next_offset)
                else
                    null;

                state.last_stable_offset = @min(@max(entry.last_stable_offset, state.log_start_offset), state.high_watermark);
                if (state.first_unstable_txn_offset) |unstable| {
                    state.last_stable_offset = @min(@max(unstable, state.log_start_offset), state.high_watermark);
                }

                const stream_id = PartitionStore.hashPartitionKey(entry.topic, entry.partition_id);
                if (self.object_manager.getStream(stream_id)) |stream| {
                    stream.advanceEndOffset(state.next_offset);
                    stream.trim(state.log_start_offset);
                }
            }
        }
    }

    /// Expire delayed fetches whose deadline has passed.
    /// Called from tick() periodically.
    fn expireDelayedFetches(self: *Broker) void {
        var i: usize = 0;
        while (i < self.delayed_fetches.items.len) {
            var df = &self.delayed_fetches.items[i];
            if (df.isExpired()) {
                // Deadline passed — remove without sending response
                // (the client will retry on timeout)
                df.deinit(self.allocator);
                _ = self.delayed_fetches.swapRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Check if any delayed fetches can be satisfied after a produce.
    /// Called after successfully producing to a partition.
    fn checkDelayedFetchesForPartition(self: *Broker, topic: []const u8, partition_id: i32) void {
        // Check if any delayed fetch is waiting on this partition
        var i: usize = 0;
        while (i < self.delayed_fetches.items.len) {
            const df = &self.delayed_fetches.items[i];
            if (df.partition_id == partition_id and std.mem.eql(u8, df.topic, topic)) {
                // Data is now available — remove from purgatory
                // The client will receive a response on its next fetch request
                // (simplified: we don't send async responses in single-threaded mode)
                df.deinit(self.allocator);
                _ = self.delayed_fetches.swapRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Write an offset commit as a RecordBatch record to __consumer_offsets.
    /// This makes offset commits durable in the same format as Java Kafka.
    /// The partition is hash(group_id) % 50.
    fn writeOffsetCommitRecord(self: *Broker, group_id: []const u8, topic: []const u8, partition_id: i32, offset: i64) void {
        // Build key: group_id + topic + partition (simple concatenation)
        const key = std.fmt.allocPrint(self.allocator, "{s}:{s}:{d}", .{ group_id, topic, partition_id }) catch return;
        defer self.allocator.free(key);

        // Build value: offset as big-endian i64
        var value_buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &value_buf, offset, .big);

        // Determine target partition in __consumer_offsets: hash(group_id) % 50
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(group_id);
        const target_partition: i32 = @intCast(hasher.final() % 50);

        // Build a RecordBatch
        const rec_batch = protocol.record_batch;
        const records = [_]rec_batch.Record{
            .{
                .offset_delta = 0,
                .key = key,
                .value = &value_buf,
            },
        };

        const batch = rec_batch.buildRecordBatch(
            self.allocator,
            0, // base_offset (will be rewritten by produce())
            &records,
            -1, // producer_id (non-transactional)
            -1, // producer_epoch
            -1, // base_sequence
            @import("time_compat").milliTimestamp(),
            @import("time_compat").milliTimestamp(),
            0, // attributes (no compression)
        ) catch return;
        defer self.allocator.free(batch);

        // Write to __consumer_offsets partition store
        _ = self.store.produce("__consumer_offsets", target_partition, batch) catch |err| {
            log.debug("Failed to write offset commit record: {}", .{err});
            return;
        };
        self.persistPartitionStates();
    }

    /// Process a raw Kafka protocol request frame and return the response.
    /// No mutex needed — single-threaded event loop.
    pub fn handleRequest(self: *Broker, request_bytes: []const u8) ?[]u8 {
        self.wireInternalPointers();

        if (request_bytes.len < 8) {
            self.json_logger.log(.warn, "Request too short", null);
            return null;
        }

        var pos: usize = 0;
        const api_key = ser.readI16(request_bytes, &pos);
        const api_version = ser.readI16(request_bytes, &pos);
        pos = 0;

        const req_header_version = header_mod.requestHeaderVersion(api_key, api_version);
        const resp_header_version = header_mod.responseHeaderVersion(api_key, api_version);

        var req_header = RequestHeader.deserialize(self.allocator, request_bytes, &pos, req_header_version) catch |err| {
            log.warn("Failed to parse request header: {}", .{err});
            self.json_logger.log(.warn, "Failed to parse request header", null);
            return null;
        };
        defer req_header.deinit(self.allocator);

        log.debug("api_key={d} v={d} corr={d}", .{ api_key, api_version, req_header.correlation_id });

        // During graceful shutdown, reject all data-path requests with NOT_LEADER_OR_FOLLOWER.
        // This tells Kafka clients to refresh metadata and reconnect to another broker.
        // ApiVersions (key 18) is still allowed so clients can detect the broker is alive
        // and perform version negotiation during reconnection.
        if (self.is_shutting_down and api_key != 18) {
            return self.handleShutdownReject(&req_header, resp_header_version);
        }

        // Auth/ACL check before dispatch
        if (self.authorizer.aclCount() > 0 or !self.authorizer.allow_everyone_if_no_acl) {
            const client_id = req_header.client_id orelse "anonymous";
            // Use authenticated principal if SASL was completed, otherwise fall back to client_id
            const principal = self.authenticated_sessions.get(client_id) orelse client_id;
            const resource_type = resourceTypeForApiKey(api_key);
            if (resource_type != .unknown) {
                const operation = operationForApiKey(api_key);
                // Extract topic name for topic-specific ACL checks (Produce/Fetch)
                const resource_name = extractTopicFromRequest(api_key, request_bytes, pos) orelse "*";
                const result = self.authorizer.authorize(principal, resource_type, resource_name, operation);
                if (result == .denied) {
                    return self.handleAuthorizationError(&req_header, resp_header_version);
                }
            }
        }

        // Validate API version is within supported range
        if (api_key != 18 and !isVersionSupported(api_key, api_version)) {
            log.warn("Unsupported version: api_key={d} v={d}", .{ api_key, api_version });
            return self.handleUnsupported(&req_header, api_key, resp_header_version);
        }

        // Track metrics — per-API and total
        self.metrics.incrementCounter("kafka_server_requests_total");
        self.metrics.addCounter("kafka_server_bytes_in_total", request_bytes.len);

        const api_name = apiKeyName(api_key);
        if (api_name.len > 0) {
            self.metrics.incrementCounter(api_name);
        }

        // Record per-API latency
        const t_start = @import("time_compat").nanoTimestamp();

        const result = switch (api_key) {
            0 => self.handleProduce(request_bytes, pos, &req_header, api_version, resp_header_version),
            1 => self.handleFetch(request_bytes, pos, &req_header, api_version, resp_header_version),
            2 => self.handleListOffsets(request_bytes, pos, &req_header, api_version, resp_header_version),
            3 => self.handleMetadata(request_bytes, pos, &req_header, api_version, resp_header_version),
            // Non-advertised legacy inter-broker RPCs. Version validation
            // rejects them until real controller-backed semantics exist.
            4 => self.handleLeaderAndIsr(request_bytes, pos, &req_header, resp_header_version),
            5 => self.handleStopReplica(request_bytes, pos, &req_header, resp_header_version),
            6 => self.handleUpdateMetadata(request_bytes, pos, &req_header, resp_header_version),
            7 => self.handleControlledShutdown(&req_header, resp_header_version),
            8 => self.handleOffsetCommit(request_bytes, pos, &req_header, api_version, resp_header_version),
            9 => self.handleOffsetFetch(request_bytes, pos, &req_header, api_version, resp_header_version),
            10 => self.handleFindCoordinator(request_bytes, pos, &req_header, api_version, resp_header_version),
            11 => self.handleJoinGroup(request_bytes, pos, &req_header, api_version, resp_header_version),
            12 => self.handleHeartbeat(request_bytes, pos, &req_header, api_version, resp_header_version),
            13 => self.handleLeaveGroup(request_bytes, pos, &req_header, api_version, resp_header_version),
            14 => self.handleSyncGroup(request_bytes, pos, &req_header, api_version, resp_header_version),
            15 => self.handleDescribeGroups(request_bytes, pos, &req_header, resp_header_version),
            16 => self.handleListGroups(&req_header, resp_header_version),
            17 => self.handleSaslHandshake(request_bytes, pos, &req_header, resp_header_version),
            18 => self.handleApiVersions(&req_header, api_version, resp_header_version),
            19 => self.handleCreateTopics(request_bytes, pos, &req_header, api_version, resp_header_version),
            20 => self.handleDeleteTopics(request_bytes, pos, &req_header, api_version, resp_header_version),
            21 => self.handleDeleteRecords(request_bytes, pos, &req_header, api_version, resp_header_version),
            22 => self.handleInitProducerId(request_bytes, pos, &req_header, api_version, resp_header_version),
            23 => self.handleOffsetForLeaderEpoch(request_bytes, pos, &req_header, api_version, resp_header_version),
            24 => self.handleAddPartitionsToTxn(request_bytes, pos, &req_header, api_version, resp_header_version),
            // AddOffsetsToTxn (key 25) — register __consumer_offsets partition in txn
            25 => self.handleAddOffsetsToTxn(request_bytes, pos, &req_header, api_version, resp_header_version),
            26 => self.handleEndTxn(request_bytes, pos, &req_header, api_version, resp_header_version),
            27 => self.handleWriteTxnMarkers(request_bytes, pos, &req_header, api_version, resp_header_version),
            28 => self.handleTxnOffsetCommit(request_bytes, pos, &req_header, api_version, resp_header_version),
            29 => self.handleDescribeAcls(&req_header, resp_header_version),
            30 => self.handleCreateAcls(request_bytes, pos, &req_header, api_version, resp_header_version),
            31 => self.handleDeleteAcls(request_bytes, pos, &req_header, api_version, resp_header_version),
            32 => self.handleDescribeConfigs(request_bytes, pos, &req_header, api_version, resp_header_version),
            33 => self.handleAlterConfigs(request_bytes, pos, &req_header, api_version, resp_header_version),
            35 => self.handleDescribeLogDirs(request_bytes, pos, &req_header, api_version, resp_header_version),
            36 => self.handleSaslAuthenticate(request_bytes, pos, &req_header, api_version, resp_header_version),
            37 => self.handleCreatePartitions(request_bytes, pos, &req_header, api_version, resp_header_version),
            42 => self.handleDeleteGroups(request_bytes, pos, &req_header, api_version, resp_header_version),
            43 => self.handleElectLeaders(request_bytes, pos, &req_header, api_version, resp_header_version),
            44 => self.handleIncrementalAlterConfigs(request_bytes, pos, &req_header, api_version, resp_header_version),
            45 => self.handleAlterPartitionReassignments(request_bytes, pos, &req_header, resp_header_version),
            46 => self.handleListPartitionReassignments(request_bytes, pos, &req_header, resp_header_version),
            // OffsetDelete (key 47) — delete committed offsets
            47 => self.handleOffsetDelete(request_bytes, pos, &req_header, api_version, resp_header_version),
            60 => self.handleDescribeCluster(&req_header, resp_header_version),
            61 => self.handleDescribeProducers(request_bytes, pos, &req_header, api_version, resp_header_version),
            52 => self.handleVote(request_bytes, pos, &req_header, api_version, resp_header_version),
            53 => self.handleBeginQuorumEpoch(request_bytes, pos, &req_header, resp_header_version),
            54 => self.handleEndQuorumEpoch(request_bytes, pos, &req_header, resp_header_version),
            55 => self.handleDescribeQuorum(&req_header, resp_header_version),
            501 => self.handleCreateStreams(request_bytes, pos, &req_header, api_version, resp_header_version),
            502 => self.handleOpenStreams(request_bytes, pos, &req_header, api_version, resp_header_version),
            503 => self.handleCloseStreams(request_bytes, pos, &req_header, api_version, resp_header_version),
            504 => self.handleDeleteStreams(request_bytes, pos, &req_header, api_version, resp_header_version),
            505 => self.handlePrepareS3Object(request_bytes, pos, &req_header, api_version, resp_header_version),
            506 => self.handleCommitStreamSetObject(request_bytes, pos, &req_header, api_version, resp_header_version),
            507 => self.handleCommitStreamObject(request_bytes, pos, &req_header, api_version, resp_header_version),
            508 => self.handleGetOpeningStreams(request_bytes, pos, &req_header, api_version, resp_header_version),
            509 => self.handleGetKVs(request_bytes, pos, &req_header, api_version, resp_header_version),
            510 => self.handlePutKVs(request_bytes, pos, &req_header, api_version, resp_header_version),
            511 => self.handleDeleteKVs(request_bytes, pos, &req_header, api_version, resp_header_version),
            512 => self.handleTrimStreams(request_bytes, pos, &req_header, api_version, resp_header_version),
            513 => self.handleAutomqRegisterNode(request_bytes, pos, &req_header, api_version, resp_header_version),
            514 => self.handleAutomqGetNodes(request_bytes, pos, &req_header, api_version, resp_header_version),
            515 => self.handleAutomqZoneRouter(request_bytes, pos, &req_header, api_version, resp_header_version),
            516 => self.handleAutomqGetPartitionSnapshot(request_bytes, pos, &req_header, api_version, resp_header_version),
            517 => self.handleUpdateLicense(request_bytes, pos, &req_header, api_version, resp_header_version),
            518 => self.handleDescribeLicense(request_bytes, pos, &req_header, api_version, resp_header_version),
            519 => self.handleExportClusterManifest(request_bytes, pos, &req_header, api_version, resp_header_version),
            600 => self.handleGetNextNodeId(request_bytes, pos, &req_header, api_version, resp_header_version),
            601 => self.handleDescribeStreams(request_bytes, pos, &req_header, api_version, resp_header_version),
            602 => self.handleAutomqUpdateGroup(request_bytes, pos, &req_header, api_version, resp_header_version),
            else => self.handleUnsupported(&req_header, api_key, resp_header_version),
        };

        const t_done = @import("time_compat").nanoTimestamp();

        // Record per-API latency in histogram
        const latency_ns = t_done - t_start;
        const latency_secs: f64 = @as(f64, @floatFromInt(@max(latency_ns, 0))) / 1_000_000_000.0;
        self.metrics.observeHistogram("kafka_server_request_latency_seconds", latency_secs);

        if (result) |resp| {
            self.metrics.addCounter("kafka_server_bytes_out_total", resp.len);

            // Track per-API error counters. Extract top-level error_code from the response.
            // Response format: correlation_id(4) [+ tagged_fields for flexible] + [throttle_time] + error_code(2).
            // We use a helper to find the error_code offset per API.
            const error_code = extractResponseErrorCode(resp, api_key, api_version, resp_header_version);
            if (error_code != 0) {
                var ec_buf: [8]u8 = undefined;
                const ec_str = std.fmt.bufPrint(&ec_buf, "{d}", .{error_code}) catch "?";
                const api_name_for_err = apiKeyName(api_key);
                const name_str = if (api_name_for_err.len > 0) api_name_for_err else "unknown";
                self.metrics.incrementLabeledCounter("kafka_server_api_errors_total", &.{ name_str, ec_str });

                // Structured JSON log for API errors
                self.json_logger.logWithFields(.warn, "API error", req_header.correlation_id, &.{ "api", name_str, "error_code", ec_str });
            }
        }

        return result;
    }

    /// Extract the top-level error_code from a Kafka response.
    /// Returns 0 if no error or if the API doesn't have a simple top-level error_code
    /// (e.g., Produce and Fetch use per-partition error codes).
    fn extractResponseErrorCode(resp: []const u8, api_key: i16, api_version: i16, resp_header_version: i16) i16 {
        // Skip correlation_id (4 bytes)
        var offset: usize = 4;

        // Flexible responses (header v1) have tagged fields after correlation_id
        if (resp_header_version >= 1) {
            // Tagged fields: varint length, typically 0x00 (1 byte for empty)
            if (offset >= resp.len) return 0;
            const tag_byte = resp[offset];
            if (tag_byte == 0) {
                offset += 1;
            } else {
                // Complex tagged fields — skip parsing, not worth it for error detection
                return 0;
            }
        }

        // AutomqUpdateGroup starts with group_id before error_code.
        if (api_key == 602) {
            var p = offset;
            const raw_len = ser.readUnsignedVarint(resp, &p) catch return 0;
            if (raw_len > 0) {
                const len = raw_len - 1;
                if (p + len > resp.len) return 0;
                p += len;
            }
            if (p + 2 > resp.len) return 0;
            return @bitCast(std.mem.readInt(u16, resp[p..][0..2], .big));
        }

        // APIs with throttle_time_ms before error_code
        const has_throttle = switch (api_key) {
            // Most APIs v1+ have throttle_time_ms; for simplicity track the common ones
            8, 9, 10, 11, 12, 13, 14, 22, 25, 26 => api_version >= 1,
            18 => false, // ApiVersions: error_code is right after header
            else => false,
        };

        if (has_throttle) {
            offset += 4; // throttle_time_ms (i32)
        }

        // APIs where we can reliably extract a top-level error_code
        switch (api_key) {
            // Produce (0), Fetch (1) — per-partition errors, skip
            0, 1 => return 0,
            // Metadata (3) — no top-level error_code
            3 => return 0,
            // These APIs have a top-level error_code at this position
            10, 11, 12, 13, 14, 18, 22, 25, 26 => {},
            // Other APIs — attempt extraction but don't fail
            else => {},
        }

        if (offset + 2 > resp.len) return 0;
        return @bitCast(std.mem.readInt(u16, resp[offset..][0..2], .big));
    }

    /// Check if an API key + version combination is supported.
    fn isVersionSupported(api_key: i16, api_version: i16) bool {
        return api_support.isBrokerVersionSupported(api_key, api_version);
    }

    fn getVersionRange(api_key: i16) api_support.VersionRange {
        return api_support.brokerVersionRange(api_key);
    }

    /// Get a human-readable name for an API key (used for metrics).
    fn apiKeyName(api_key: i16) []const u8 {
        return api_support.brokerMetricName(api_key);
    }

    /// Map API key to the corresponding ACL resource type (fix #7).
    fn resourceTypeForApiKey(api_key: i16) Authorizer.ResourceType {
        return switch (api_key) {
            0, 1, 2 => .topic, // Produce, Fetch, ListOffsets
            8, 9, 10, 11, 12, 13, 14, 15, 16, 42, 47 => .group, // group-related
            22, 24, 26, 27, 28 => .transactional_id, // txn-related
            3, 18, 19, 20, 32, 33, 35, 37, 43, 44, 45, 46 => .cluster, // metadata/admin
            501...519, 600...602 => .cluster, // AutoMQ stream/object/controller extensions
            else => .unknown,
        };
    }

    /// Map API key to the corresponding ACL operation (fix #7).
    fn operationForApiKey(api_key: i16) Authorizer.Operation {
        return switch (api_key) {
            0 => .write, // Produce
            1 => .read, // Fetch
            2, 9, 15, 16, 23, 29, 32, 35, 46, 55, 60, 61 => .describe, // ListOffsets, OffsetFetch, Describe*
            3, 18 => .describe, // Metadata, ApiVersions
            8 => .read, // OffsetCommit
            10, 11, 12, 13, 14 => .read, // Group ops
            19, 37 => .create, // CreateTopics, CreatePartitions
            20, 42, 47 => .delete, // DeleteTopics, DeleteGroups, OffsetDelete
            22, 24, 26, 27, 28 => .write, // Txn ops
            30 => .alter, // CreateAcls
            31 => .alter, // DeleteAcls
            33, 43, 44, 45 => .alter, // Alter/admin APIs
            501, 502, 503, 508, 514, 516, 518, 601 => .describe, // AutoMQ reads/lifecycle probes
            504, 511 => .delete, // AutoMQ deletes
            505, 506, 507, 510, 512, 513, 515, 517, 519, 600, 602 => .alter, // AutoMQ mutations
            else => .any,
        };
    }

    /// Return an authorization error response (fix #7).
    fn handleAuthorizationError(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(buf, &wpos, resp_header_version);
        // Error code 29 = TOPIC_AUTHORIZATION_FAILED
        ser.writeI16(buf, &wpos, 29);
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    /// Extract the first topic name from a Produce or Fetch request for topic-specific ACL checks.
    /// Returns null for non-topic APIs or if parsing fails.
    fn extractTopicFromRequest(api_key: i16, request_bytes: []const u8, body_start: usize) ?[]const u8 {
        if (api_key != 0 and api_key != 1) return null; // Only Produce (0) and Fetch (1)
        var pos = body_start;
        if (api_key == 0) {
            // Produce v0+: transactional_id (nullable string), acks (i16), timeout (i32), then topic array
            _ = ser.readString(request_bytes, &pos) catch return null; // transactional_id
            if (pos + 6 > request_bytes.len) return null;
            pos += 2; // acks (i16)
            pos += 4; // timeout_ms (i32)
            const num_topics_opt = ser.readArrayLen(request_bytes, &pos) catch return null;
            if (num_topics_opt) |num_topics| {
                if (num_topics > 0) {
                    return (ser.readString(request_bytes, &pos) catch null) orelse null;
                }
            }
        } else {
            // Fetch v0+: replica_id (i32), max_wait (i32), min_bytes (i32), then topic array
            if (pos + 12 > request_bytes.len) return null;
            pos += 4; // replica_id (i32)
            pos += 4; // max_wait_ms (i32)
            pos += 4; // min_bytes (i32)
            const num_topics_opt = ser.readArrayLen(request_bytes, &pos) catch return null;
            if (num_topics_opt) |num_topics| {
                if (num_topics > 0) {
                    return (ser.readString(request_bytes, &pos) catch null) orelse null;
                }
            }
        }
        return null;
    }

    // ---------------------------------------------------------------
    // ApiVersions (key 18)
    // ---------------------------------------------------------------
    fn handleApiVersions(self: *Broker, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const supported = api_support.broker_supported_apis;
        var api_keys_list: [supported.len]ApiVersionsResponse.ApiVersion = undefined;
        for (supported, 0..) |s, i| {
            api_keys_list[i] = .{ .api_key = s.key, .min_version = s.min, .max_version = s.max };
        }

        const resp_body = ApiVersionsResponse{
            .error_code = 0,
            .api_keys = &api_keys_list,
            .throttle_time_ms = 0,
        };

        const body_version: i16 = @min(api_version, 4);
        return self.serializeResponse(req_header, resp_header_version, &resp_body, body_version);
    }

    fn serializeResponse(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16, resp_body: *const ApiVersionsResponse, body_version: i16) ?[]u8 {
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const header_size = resp_header.calcSize(resp_header_version);
        const body_size = resp_body.calcSize(body_version);
        const total_size = header_size + body_size;

        const buf = self.allocator.alloc(u8, total_size) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        resp_body.serialize(buf, &wpos, body_version);
        return buf;
    }

    fn serializeGeneratedResponse(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16, resp_body: anytype, body_version: i16) ?[]u8 {
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const header_size = resp_header.calcSize(resp_header_version);
        const body_size = resp_body.calcSize(body_version);
        const total_size = header_size + body_size;

        const buf = self.allocator.alloc(u8, total_size) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        resp_body.serialize(buf, &wpos, body_version);
        return buf[0..wpos];
    }

    fn parseGeneratedRequest(comptime RequestType: type, allocator: Allocator, request_bytes: []const u8, body_start: usize, api_version: i16) !RequestType {
        if (body_start > request_bytes.len) return error.BufferUnderflow;
        var pos = body_start;
        return RequestType.deserialize(allocator, request_bytes, &pos, api_version);
    }

    fn errorCode(code: ErrorCode) i16 {
        return code.toInt();
    }

    fn invalidStreamId(stream_id: i64) bool {
        return stream_id < 0;
    }

    fn i64ToU64(value: i64) ?u64 {
        if (value < 0) return null;
        return @intCast(value);
    }

    fn u64ToI64(value: u64) i64 {
        if (value > @as(u64, @intCast(std.math.maxInt(i64)))) return std.math.maxInt(i64);
        return @intCast(value);
    }

    fn streamErrorCode(err: anyerror) i16 {
        return switch (err) {
            error.StreamNotFound => errorCode(.resource_not_found),
            error.DuplicateStream => errorCode(.duplicate_resource),
            error.StaleStreamEpoch => errorCode(.fenced_leader_epoch),
            error.OffsetOutOfRange => errorCode(.position_out_of_range),
            else => errorCode(.kafka_storage_error),
        };
    }

    fn makeStreamObjectKey(self: *Broker, object_id: u64, stream_id: u64, start_offset: u64, end_offset: u64) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "so/{d}/{d}-{d}-{d}", .{ stream_id, start_offset, end_offset, object_id });
    }

    fn makeStreamSetObjectKey(self: *Broker, object_id: u64, node_id: i32) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "sso/{d}/{d}", .{ node_id, object_id });
    }

    fn hasOpeningStreamsForNode(self: *Broker, node_id: i32) bool {
        var it = self.object_manager.streams.iterator();
        while (it.next()) |entry| {
            const stream = entry.value_ptr;
            if (stream.node_id == node_id and stream.state == .opened) return true;
        }
        return false;
    }

    fn findTopicPartitionForStream(self: *Broker, stream_id: u64) ?struct { topic_id: [16]u8, topic_name: []const u8, partition_index: i32 } {
        var topic_it = self.topics.iterator();
        while (topic_it.next()) |entry| {
            const info = entry.value_ptr;
            var pi: i32 = 0;
            while (pi < info.num_partitions) : (pi += 1) {
                if (PartitionStore.hashPartitionKey(info.name, pi) == stream_id) {
                    return .{
                        .topic_id = info.topic_id,
                        .topic_name = info.name,
                        .partition_index = pi,
                    };
                }
            }
        }
        return null;
    }

    // ---------------------------------------------------------------
    // Metadata (key 3) — Single-node mode
    // In single-node mode, this broker is the ONLY broker in the cluster.
    // All partitions have leader = self.node_id and ISR = [self.node_id].
    // Multi-broker Raft replication is NOT implemented — this is documented
    // as an intentional limitation. When multi-broker support is added,
    // the metadata response will be populated from the Raft cluster state.
    // ---------------------------------------------------------------
    fn handleMetadata(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const metadata_mod = protocol.messages.metadata;
        const MetadataResponse = metadata_mod.MetadataResponse;

        // Parse requested topics
        var pos = body_start;
        const flexible = api_version >= 9;
        var requested_all = true;
        var requested_topics = std.array_list.Managed([]const u8).init(self.allocator);
        defer requested_topics.deinit();

        const num_req_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0
        else blk: {
            const n = ser.readI32(request_bytes, &pos);
            if (n < 0) break :blk @as(usize, 0); // null = all topics
            break :blk @as(usize, @intCast(n));
        };

        if (num_req_topics > 0) {
            requested_all = false;
            for (0..num_req_topics) |_| {
                const tn = if (flexible)
                    (ser.readCompactString(request_bytes, &pos) catch break) orelse ""
                else
                    (ser.readString(request_bytes, &pos) catch break) orelse "";
                if (tn.len > 0) requested_topics.append(tn) catch {};
            }
        }

        var brokers = [_]MetadataResponse.MetadataResponseBroker{
            .{ .node_id = self.node_id, .host = self.advertised_host, .port = @intCast(self.port) },
        };

        // Build topic metadata from our known topics
        var topic_list = std.array_list.Managed(MetadataResponse.MetadataResponseTopic).init(self.allocator);
        defer topic_list.deinit();

        // Auto-create requested topics that don't exist
        if (!requested_all) {
            for (requested_topics.items) |rt| {
                _ = self.ensureTopic(rt);
            }
        }

        var topics_iter = self.topics.iterator();
        while (topics_iter.next()) |entry| {
            const info = entry.value_ptr;

            // Filter: if specific topics were requested, only include those
            if (!requested_all) {
                var found = false;
                for (requested_topics.items) |rt| {
                    if (std.mem.eql(u8, rt, info.name)) {
                        found = true;
                        break;
                    }
                }
                if (!found) continue;
            }
            var parts = std.array_list.Managed(MetadataResponse.MetadataResponsePartition).init(self.allocator);
            defer parts.deinit();

            for (0..@intCast(info.num_partitions)) |pi| {
                var replicas = [_]i32{self.node_id};
                var isr = [_]i32{self.node_id};
                parts.append(.{
                    .partition_index = @intCast(pi),
                    .leader_id = self.node_id,
                    .leader_epoch = if (self.raft_state) |rs| rs.current_epoch else self.cached_leader_epoch,
                    .replica_nodes = &replicas,
                    .isr_nodes = &isr,
                }) catch continue;
            }

            topic_list.append(.{
                .name = info.name,
                .topic_id = info.topic_id,
                .is_internal = std.mem.startsWith(u8, info.name, "__"),
                .partitions = parts.toOwnedSlice() catch &.{},
            }) catch continue;
        }

        const resp_body = MetadataResponse{
            .brokers = &brokers,
            .cluster_id = "zmq-cluster",
            .controller_id = self.node_id,
            .topics = topic_list.items,
        };

        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        // Dynamic buffer — topics can be large
        const estimated_size = 256 + self.topics.count() * 128;
        const buf = self.allocator.alloc(u8, @max(estimated_size, 8192)) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        const body_version: i16 = @min(api_version, 12);
        resp_body.serialize(buf, &wpos, body_version);

        // Free partition slices we allocated
        for (topic_list.items) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // Produce (key 0)
    // ---------------------------------------------------------------
    fn handleProduce(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        // Reject all produces if this broker has been fenced by the controller.
        // Clients will receive NOT_LEADER_OR_FOLLOWER and refresh metadata.
        if (self.is_fenced_by_controller) {
            self.json_logger.log(.warn, "Produce rejected: broker fenced by controller", req_header.correlation_id);
            return self.handleNotController(req_header, resp_header_version);
        }

        var pos = body_start;
        const flexible = api_version >= 9;

        // Parse transactional_id (v3+)
        if (api_version >= 3) {
            if (flexible) {
                _ = ser.readCompactString(request_bytes, &pos) catch return null;
            } else {
                _ = ser.readString(request_bytes, &pos) catch return null;
            }
        }

        // Bounds-check before unchecked integer reads
        if (pos + 10 > request_bytes.len) {
            log.warn("Produce request too short for header fields: need {d} bytes at pos {d}, have {d}", .{ 10, pos, request_bytes.len });
            return null;
        }

        const acks = ser.readI16(request_bytes, &pos);
        _ = ser.readI32(request_bytes, &pos); // timeout_ms

        // acks=-1 semantics — in single-node mode, self is the only ISR member,
        // so acks=-1 (all replicas) behaves identically to acks=1.
        if (acks == -1 and self.default_replication_factor > 1) {
            log.warn("acks=-1 with replication_factor={d}: single-node mode treats as acks=1", .{self.default_replication_factor});
        }

        // Topics array
        const num_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));

        // Build response — right-sized for typical produce responses.
        const produce_resp_size = @max(@as(usize, 128), 16 + num_topics * (20 + 6 * 30));
        const resp_buf = self.allocator.alloc(u8, produce_resp_size) catch return null;
        var resp_buf_owned = true;
        defer if (resp_buf_owned) self.allocator.free(resp_buf);

        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);

        // Responses array header
        if (flexible) {
            ser.writeCompactArrayLen(resp_buf, &wpos, num_topics);
        } else {
            ser.writeArrayLen(resp_buf, &wpos, num_topics);
        }

        var object_metadata_dirty = false;
        var partition_state_dirty = false;
        for (0..num_topics) |_| {
            // Read topic name
            const topic_name = if (flexible)
                (ser.readCompactString(request_bytes, &pos) catch return null) orelse ""
            else
                (ser.readString(request_bytes, &pos) catch return null) orelse "";

            // Write topic name in response
            if (flexible) {
                ser.writeCompactString(resp_buf, &wpos, topic_name);
            } else {
                ser.writeString(resp_buf, &wpos, topic_name);
            }

            // Partitions array
            const num_partitions = if (flexible)
                (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
            else blk: {
                if (pos + 4 > request_bytes.len) return null;
                break :blk @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
            };

            if (flexible) {
                ser.writeCompactArrayLen(resp_buf, &wpos, num_partitions);
            } else {
                ser.writeArrayLen(resp_buf, &wpos, num_partitions);
            }

            for (0..num_partitions) |_| {
                if (pos + 4 > request_bytes.len) return null;
                const partition_idx = ser.readI32(request_bytes, &pos);

                // Read records (bytes)
                const records = if (flexible)
                    (ser.readCompactBytes(request_bytes, &pos) catch return null)
                else
                    (ser.readBytes(request_bytes, &pos) catch return null);

                // Idempotent producer dedup + CRC validation
                var is_duplicate = false;
                var crc_valid = true;
                if (records) |rec| {
                    if (rec.len >= 53) { // Minimum V2 record batch header
                        const rec_batch = protocol.record_batch;
                        const crc32c = @import("core").crc32c;
                        const batch_header = rec_batch.RecordBatchHeader.parse(rec) catch null;
                        if (batch_header) |hdr| {
                            // CRC-32C validation: covers bytes from attributes (offset 21) to end
                            if (hdr.crc != 0 and rec.len > 21) {
                                const computed_crc = crc32c.compute(rec[21..]);
                                if (computed_crc != @as(u32, @bitCast(hdr.crc))) {
                                    crc_valid = false;
                                    log.warn("CRC mismatch on produce: expected={x} computed={x}", .{
                                        @as(u32, @bitCast(hdr.crc)), computed_crc,
                                    });
                                }
                            }

                            // Idempotent dedup
                            if (hdr.producer_id >= 0) {
                                const pk = ProducerKey{
                                    .producer_id = hdr.producer_id,
                                    .partition_key = PartitionStore.hashPartitionKey(topic_name, partition_idx),
                                };
                                if (self.producer_sequences.get(pk)) |stored| {
                                    if (hdr.producer_epoch < stored.producer_epoch) {
                                        // Stale epoch — producer was fenced
                                        is_duplicate = true; // Reject stale-epoch records
                                    } else if (hdr.producer_epoch > stored.producer_epoch) {
                                        // New epoch — accept (producer restarted)
                                    } else {
                                        // Same epoch — check sequence
                                        if (hdr.base_sequence <= stored.last_sequence) {
                                            is_duplicate = true;
                                        }
                                    }
                                }
                                if (!is_duplicate) {
                                    self.producer_sequences.put(pk, .{
                                        .last_sequence = hdr.base_sequence,
                                        .producer_epoch = hdr.producer_epoch,
                                    }) catch {};
                                    self.producer_sequences_dirty = true;
                                }
                            }
                        }
                    }
                }

                // Auto-create topic if needed
                _ = self.ensureTopic(topic_name);
                object_metadata_dirty = true;

                // Actually produce (skip if duplicate or CRC invalid)
                var was_wal_fenced = false;
                var was_storage_error = false;
                const produce_result = if (is_duplicate or !crc_valid)
                    null
                else if (records) |rec|
                    self.store.produce(topic_name, partition_idx, rec) catch |err| blk: {
                        // Map storage errors to proper Kafka error codes
                        if (err == error.MessageTooLarge) {
                            log.warn("Record batch too large for {s}-{d}", .{ topic_name, partition_idx });
                        } else if (err == error.WalFenced) {
                            log.warn("WAL fenced: rejecting produce to {s}-{d} (broker is no longer leader)", .{ topic_name, partition_idx });
                            was_wal_fenced = true;
                        } else if (err == error.S3WalFlushFailed or err == error.S3StorageUnavailable) {
                            log.warn("S3 WAL storage error for {s}-{d}: {}", .{ topic_name, partition_idx, err });
                            was_storage_error = true;
                        }
                        break :blk null;
                    }
                else
                    null;

                // Detect MessageTooLarge specifically for proper error code
                const produce_error_code: i16 = if (is_duplicate)
                    0 // idempotent success
                else if (!crc_valid)
                    2 // CORRUPT_MESSAGE
                else if (produce_result != null)
                    0 // success
                else if (was_wal_fenced)
                    6 // NOT_LEADER_OR_FOLLOWER — tells client to find the new leader
                else if (was_storage_error)
                    56 // KAFKA_STORAGE_ERROR
                else if (records) |rec| blk: {
                    if (rec.len > 1048576) break :blk 10; // MESSAGE_TOO_LARGE
                    break :blk 1; // OFFSET_OUT_OF_RANGE (generic failure)
                } else 1; // OFFSET_OUT_OF_RANGE

                // Notify delayed fetches when new data arrives
                if (produce_result != null) {
                    self.checkDelayedFetchesForPartition(topic_name, partition_idx);
                    object_metadata_dirty = true;
                    partition_state_dirty = true;
                }

                // Write partition response with proper error codes
                ser.writeI32(resp_buf, &wpos, partition_idx); // index
                if (is_duplicate) {
                    // Duplicate detection — return success (idempotent)
                    ser.writeI16(resp_buf, &wpos, 0);
                    ser.writeI64(resp_buf, &wpos, -1);
                    if (api_version >= 2) ser.writeI64(resp_buf, &wpos, -1);
                    if (api_version >= 5) ser.writeI64(resp_buf, &wpos, -1);
                } else if (produce_result) |result| {
                    ser.writeI16(resp_buf, &wpos, 0); // no error
                    ser.writeI64(resp_buf, &wpos, result.base_offset);
                    if (api_version >= 2) ser.writeI64(resp_buf, &wpos, result.log_append_time_ms);
                    if (api_version >= 5) ser.writeI64(resp_buf, &wpos, result.log_start_offset);
                } else {
                    ser.writeI16(resp_buf, &wpos, produce_error_code);
                    ser.writeI64(resp_buf, &wpos, -1);
                    if (api_version >= 2) ser.writeI64(resp_buf, &wpos, -1);
                    if (api_version >= 5) ser.writeI64(resp_buf, &wpos, -1);
                }

                if (api_version >= 8) {
                    // RecordErrors (empty) + ErrorMessage (null)
                    if (flexible) {
                        ser.writeCompactArrayLen(resp_buf, &wpos, 0);
                        ser.writeCompactString(resp_buf, &wpos, null);
                    } else {
                        ser.writeArrayLen(resp_buf, &wpos, 0);
                        ser.writeString(resp_buf, &wpos, null);
                    }
                }

                if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
            }

            if (flexible) {
                ser.skipTaggedFields(request_bytes, &pos) catch {};
                ser.writeEmptyTaggedFields(resp_buf, &wpos);
            }
        }

        // ThrottleTimeMs (v1+) — enforce produce quotas
        if (api_version >= 1) {
            const client_id_str = req_header.client_id orelse "unknown";
            const throttle = self.quota_manager.recordProduce(client_id_str, request_bytes.len);
            ser.writeI32(resp_buf, &wpos, throttle);
            // Track throttle metrics
            if (throttle > 0) {
                self.metrics.incrementCounter("kafka_server_produce_throttle_total");
                log.debug("Produce throttled {d}ms for client {s}", .{ throttle, client_id_str });
            }
        }

        if (flexible) {
            ser.skipTaggedFields(request_bytes, &pos) catch {};
            ser.writeEmptyTaggedFields(resp_buf, &wpos);
        }

        log.debug("Produce: {d} topics, acks={d}, response {d} bytes", .{ num_topics, acks, wpos });
        if (partition_state_dirty) self.persistPartitionStates();
        if (object_metadata_dirty) self.persistObjectManagerSnapshot();

        // acks=0: fire-and-forget — don't send a response
        if (acks == 0) {
            // resp_buf_owned is true, defer will free resp_buf
            return null;
        }

        // Transfer ownership to caller: shrink to exact size needed.
        // realloc cannot fail when shrinking with the GPA, but if it somehow
        // does the defer will free resp_buf and we return null (no response).
        const result = self.allocator.realloc(resp_buf, wpos) catch return null;
        resp_buf_owned = false;
        return result;
    }

    // ---------------------------------------------------------------
    // Fetch (key 1)
    // ---------------------------------------------------------------
    fn handleFetch(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 12;

        // Skip replica_id, max_wait_ms, min_bytes
        if (api_version <= 14) _ = ser.readI32(request_bytes, &pos); // replica_id
        const max_wait_ms = ser.readI32(request_bytes, &pos); // max_wait_ms
        _ = max_wait_ms; // Used for delayed fetch timeout (simplified implementation)
        _ = ser.readI32(request_bytes, &pos); // min_bytes
        if (api_version >= 3) _ = ser.readI32(request_bytes, &pos); // max_bytes
        // Parse isolation_level for READ_COMMITTED support
        var isolation_level: i8 = 0;
        if (api_version >= 4) isolation_level = ser.readI8(request_bytes, &pos);
        var session_id: i32 = 0;
        var session_epoch: i32 = 0;
        if (api_version >= 7) {
            session_id = ser.readI32(request_bytes, &pos);
            session_epoch = ser.readI32(request_bytes, &pos);
        }

        // Manage fetch session (KIP-227)
        const session_result = self.fetch_sessions.getOrCreate(session_id, session_epoch) catch null;
        const resp_session_id: i32 = if (session_result) |sr| sr.session_id else 0;

        // Topics array
        const num_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));

        const resp_buf = self.allocator.alloc(u8, 1024 * 1024) catch return null; // 1MB initial
        var resp_buf_owned = true;
        defer if (resp_buf_owned) self.allocator.free(resp_buf);

        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);

        // ThrottleTimeMs (v1+) — enforce fetch quotas
        if (api_version >= 1) {
            const client_id_str = req_header.client_id orelse "unknown";
            const throttle = self.quota_manager.recordFetch(client_id_str, request_bytes.len);
            ser.writeI32(resp_buf, &wpos, throttle);
            if (throttle > 0) {
                self.metrics.incrementCounter("kafka_server_fetch_throttle_total");
                log.debug("Fetch throttled {d}ms for client {s}", .{ throttle, client_id_str });
            }
        }
        // ErrorCode (v7+)
        if (api_version >= 7) ser.writeI16(resp_buf, &wpos, 0);
        // SessionId (v7+) — return the fetch session ID
        if (api_version >= 7) ser.writeI32(resp_buf, &wpos, resp_session_id);

        // Responses array
        if (flexible) {
            ser.writeCompactArrayLen(resp_buf, &wpos, num_topics);
        } else {
            ser.writeArrayLen(resp_buf, &wpos, num_topics);
        }

        for (0..num_topics) |_| {
            // Read topic name/id
            var topic_name: []const u8 = "";
            if (api_version <= 12) {
                if (flexible) {
                    topic_name = (ser.readCompactString(request_bytes, &pos) catch return null) orelse "";
                } else {
                    topic_name = (ser.readString(request_bytes, &pos) catch return null) orelse "";
                }
            }
            if (api_version >= 13) {
                _ = ser.readUuid(request_bytes, &pos) catch return null; // topic_id
            }

            // Write topic in response
            if (api_version <= 12) {
                if (flexible) ser.writeCompactString(resp_buf, &wpos, topic_name) else ser.writeString(resp_buf, &wpos, topic_name);
            }
            if (api_version >= 13) {
                ser.writeUuid(resp_buf, &wpos, [_]u8{0} ** 16);
            }

            // Partitions
            const num_partitions = if (flexible)
                (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
            else
                @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));

            if (flexible) {
                ser.writeCompactArrayLen(resp_buf, &wpos, num_partitions);
            } else {
                ser.writeArrayLen(resp_buf, &wpos, num_partitions);
            }

            for (0..num_partitions) |_| {
                const partition_idx = ser.readI32(request_bytes, &pos);
                if (api_version >= 9) _ = ser.readI32(request_bytes, &pos); // current_leader_epoch
                const fetch_offset: u64 = @intCast(ser.readI64(request_bytes, &pos));
                if (api_version >= 12) _ = ser.readI32(request_bytes, &pos); // last_fetched_epoch
                if (api_version >= 5) _ = ser.readI64(request_bytes, &pos); // log_start_offset
                _ = ser.readI32(request_bytes, &pos); // partition_max_bytes

                if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch {};

                // Fetch data with isolation level
                const fetch_result = self.store.fetchWithIsolation(topic_name, partition_idx, fetch_offset, 1024 * 1024, isolation_level) catch |err| blk: {
                    log.debug("Fetch failed for {s}-{d} at offset {d}: {}", .{ topic_name, partition_idx, fetch_offset, err });
                    break :blk PartitionStore.FetchResult{
                        .error_code = 3,
                        .records = &.{},
                        .high_watermark = 0,
                        .last_stable_offset = -1,
                    };
                };

                // Write partition response
                ser.writeI32(resp_buf, &wpos, partition_idx);
                ser.writeI16(resp_buf, &wpos, fetch_result.error_code);
                ser.writeI64(resp_buf, &wpos, fetch_result.high_watermark);

                // Return actual last_stable_offset instead of -1
                if (api_version >= 4) ser.writeI64(resp_buf, &wpos, fetch_result.last_stable_offset);
                if (api_version >= 5) ser.writeI64(resp_buf, &wpos, 0); // log_start_offset
                if (api_version >= 4) {
                    // Aborted transactions (empty)
                    if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, 0) else ser.writeArrayLen(resp_buf, &wpos, 0);
                }
                if (api_version >= 11) ser.writeI32(resp_buf, &wpos, -1); // preferred_read_replica

                // Records bytes
                if (fetch_result.records.len > 0) {
                    if (flexible) {
                        ser.writeCompactBytes(resp_buf, &wpos, fetch_result.records);
                    } else {
                        ser.writeBytesBuf(resp_buf, &wpos, fetch_result.records);
                    }
                    self.allocator.free(@constCast(fetch_result.records));
                } else {
                    if (flexible) ser.writeCompactBytes(resp_buf, &wpos, null) else ser.writeBytesBuf(resp_buf, &wpos, null);
                }

                if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
            }

            if (flexible) {
                ser.skipTaggedFields(request_bytes, &pos) catch {};
                ser.writeEmptyTaggedFields(resp_buf, &wpos);
            }
        }

        if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);

        log.info("Fetch: {d} topics, response {d} bytes", .{ num_topics, wpos });
        const result = self.allocator.realloc(resp_buf, wpos) catch return null;
        resp_buf_owned = false;
        return result;
    }

    // ---------------------------------------------------------------
    // ListOffsets (key 2)
    // ---------------------------------------------------------------
    fn handleListOffsets(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 6;
        _ = ser.readI32(request_bytes, &pos); // replica_id
        if (api_version >= 2) _ = ser.readI8(request_bytes, &pos); // isolation_level

        const num_req_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0
        else
            (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var resp_buf = self.allocator.alloc(u8, 8192) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 2) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms

        if (num_req_topics > 0) {
            if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_req_topics) else ser.writeArrayLen(resp_buf, &wpos, num_req_topics);
            for (0..num_req_topics) |_| {
                const topic_name = if (flexible)
                    ((ser.readCompactString(request_bytes, &pos) catch break) orelse "")
                else
                    ((ser.readString(request_bytes, &pos) catch break) orelse "");
                if (flexible) ser.writeCompactString(resp_buf, &wpos, topic_name) else ser.writeString(resp_buf, &wpos, topic_name);

                const num_parts = if (flexible)
                    (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0
                else
                    (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
                if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_parts) else ser.writeArrayLen(resp_buf, &wpos, num_parts);

                for (0..num_parts) |_| {
                    const part_idx = ser.readI32(request_bytes, &pos);
                    if (api_version >= 4) _ = ser.readI32(request_bytes, &pos); // current_leader_epoch
                    const timestamp = ser.readI64(request_bytes, &pos);
                    if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch {};

                    const pkey = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ topic_name, part_idx }) catch continue;
                    defer self.allocator.free(pkey);
                    const hw: i64 = if (self.store.partitions.get(pkey)) |state| @intCast(state.high_watermark) else 0;

                    ser.writeI32(resp_buf, &wpos, part_idx);
                    ser.writeI16(resp_buf, &wpos, 0); // error_code
                    if (api_version == 0) {
                        ser.writeI64(resp_buf, &wpos, -1); // old_style_offsets (not used)
                    }
                    ser.writeI64(resp_buf, &wpos, -1); // timestamp
                    ser.writeI64(resp_buf, &wpos, if (timestamp == -2) @as(i64, 0) else hw);
                    if (api_version >= 4) ser.writeI32(resp_buf, &wpos, if (self.raft_state) |rs| rs.current_epoch else self.cached_leader_epoch); // leader_epoch
                    if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
                }
                if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
            }
        } else {
            if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, 0) else ser.writeArrayLen(resp_buf, &wpos, 0);
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // FindCoordinator (key 10)
    // ---------------------------------------------------------------
    fn handleFindCoordinator(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 3;
        // Parse the coordinator key
        const key = if (flexible)
            (ser.readCompactString(request_bytes, &pos) catch null) orelse ""
        else
            (ser.readString(request_bytes, &pos) catch null) orelse "";
        _ = key;

        var resp_buf = self.allocator.alloc(u8, 256) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 1) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms (v1+)
        ser.writeI16(resp_buf, &wpos, 0); // error_code
        if (api_version >= 1) {
            if (flexible) ser.writeCompactString(resp_buf, &wpos, null) else ser.writeString(resp_buf, &wpos, null); // error_message
        }
        ser.writeI32(resp_buf, &wpos, self.node_id); // node_id
        if (flexible) {
            ser.writeCompactString(resp_buf, &wpos, self.advertised_host);
        } else {
            ser.writeString(resp_buf, &wpos, self.advertised_host);
        }
        ser.writeI32(resp_buf, &wpos, @intCast(self.port)); // port
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // JoinGroup (key 11)
    // ---------------------------------------------------------------
    fn handleJoinGroup(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 6;
        const readStr = if (flexible) &ser.readCompactString else &ser.readString;
        const group_id = (readStr(request_bytes, &pos) catch return null) orelse "";
        _ = ser.readI32(request_bytes, &pos); // session_timeout
        if (api_version >= 1) _ = ser.readI32(request_bytes, &pos); // rebalance_timeout
        const req_member_id = readStr(request_bytes, &pos) catch null;
        // Parse group_instance_id for static membership
        var group_instance_id: ?[]const u8 = null;
        if (api_version >= 5) group_instance_id = readStr(request_bytes, &pos) catch null;
        const protocol_type = (readStr(request_bytes, &pos) catch null) orelse "consumer";
        // Skip protocols array
        const num_protocols = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
        _ = num_protocols;

        const result = self.groups.joinGroupWithInstanceId(group_id, req_member_id, group_instance_id, protocol_type, null) catch return null;

        var resp_buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 2) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(resp_buf, &wpos, result.error_code);
        ser.writeI32(resp_buf, &wpos, result.generation_id);
        if (api_version >= 7) {
            if (flexible) ser.writeCompactString(resp_buf, &wpos, "consumer") else ser.writeString(resp_buf, &wpos, "consumer");
        } else {
            ser.writeString(resp_buf, &wpos, "consumer");
        }
        if (flexible) {
            ser.writeCompactString(resp_buf, &wpos, if (result.leader_id) |lid| lid else "");
            ser.writeCompactString(resp_buf, &wpos, result.member_id);
        } else {
            ser.writeString(resp_buf, &wpos, if (result.leader_id) |lid| lid else "");
            ser.writeString(resp_buf, &wpos, result.member_id);
        }

        // Members list
        if (result.is_leader) {
            const group = self.groups.groups.getPtr(group_id);
            if (group) |g| {
                if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, g.members.count()) else ser.writeArrayLen(resp_buf, &wpos, g.members.count());
                var it = g.members.iterator();
                while (it.next()) |entry| {
                    if (flexible) ser.writeCompactString(resp_buf, &wpos, entry.key_ptr.*) else ser.writeString(resp_buf, &wpos, entry.key_ptr.*);
                    if (api_version >= 5) {
                        if (flexible) ser.writeCompactString(resp_buf, &wpos, null) else ser.writeString(resp_buf, &wpos, null); // group_instance_id
                    }
                    if (flexible) ser.writeCompactBytes(resp_buf, &wpos, null) else ser.writeBytesBuf(resp_buf, &wpos, null); // metadata
                    if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
                }
            } else {
                if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, 0) else ser.writeArrayLen(resp_buf, &wpos, 0);
            }
        } else {
            if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, 0) else ser.writeArrayLen(resp_buf, &wpos, 0);
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // Heartbeat (key 12)
    // ---------------------------------------------------------------
    fn handleHeartbeat(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 4;
        const group_id = if (flexible)
            ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
        else
            ((ser.readString(request_bytes, &pos) catch return null) orelse "");
        const generation_id = ser.readI32(request_bytes, &pos);
        const member_id = if (flexible)
            ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
        else
            ((ser.readString(request_bytes, &pos) catch return null) orelse "");
        if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch {};

        const error_code = self.groups.heartbeat(group_id, member_id, generation_id);

        var resp_buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 1) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(resp_buf, &wpos, error_code);
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // LeaveGroup (key 13)
    // ---------------------------------------------------------------
    fn handleLeaveGroup(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 4;
        const group_id = if (flexible)
            ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
        else
            ((ser.readString(request_bytes, &pos) catch return null) orelse "");
        const member_id = if (flexible)
            ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
        else
            ((ser.readString(request_bytes, &pos) catch return null) orelse "");

        const error_code = self.groups.leaveGroup(group_id, member_id);

        var resp_buf = self.allocator.alloc(u8, 128) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 1) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(resp_buf, &wpos, error_code);
        if (api_version >= 3) {
            // v3+: members array in response
            if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, 0) else ser.writeArrayLen(resp_buf, &wpos, 0);
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // OffsetCommit (key 8)
    // ---------------------------------------------------------------
    fn handleOffsetCommit(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 8;
        const group_id = if (flexible)
            ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
        else
            ((ser.readString(request_bytes, &pos) catch return null) orelse "");
        _ = ser.readI32(request_bytes, &pos); // generation_id
        if (flexible) {
            _ = ser.readCompactString(request_bytes, &pos) catch return null; // member_id
        } else {
            _ = ser.readString(request_bytes, &pos) catch return null; // member_id
        }

        const num_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));

        var resp_buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 3) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_topics) else ser.writeArrayLen(resp_buf, &wpos, num_topics);

        for (0..num_topics) |_| {
            const topic_name = if (flexible)
                ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
            else
                ((ser.readString(request_bytes, &pos) catch return null) orelse "");
            if (flexible) ser.writeCompactString(resp_buf, &wpos, topic_name) else ser.writeString(resp_buf, &wpos, topic_name);

            const num_partitions = if (flexible)
                (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
            else
                @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
            if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_partitions) else ser.writeArrayLen(resp_buf, &wpos, num_partitions);

            for (0..num_partitions) |_| {
                const partition_id = ser.readI32(request_bytes, &pos);
                const offset = ser.readI64(request_bytes, &pos);

                // Look up the log end offset (next_offset) for lag computation
                const leo: ?i64 = blk: {
                    const stream_key = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ topic_name, partition_id }) catch break :blk null;
                    defer self.allocator.free(stream_key);
                    if (self.store.partitions.getPtr(stream_key)) |state| {
                        break :blk @intCast(state.next_offset);
                    }
                    break :blk null;
                };

                self.groups.commitOffsetWithLag(group_id, topic_name, partition_id, offset, leo) catch {};

                // Write offset commit as a RecordBatch to __consumer_offsets
                // The partition in __consumer_offsets is determined by hash(group_id) % 50
                self.writeOffsetCommitRecord(group_id, topic_name, partition_id, offset);

                ser.writeI32(resp_buf, &wpos, partition_id);
                ser.writeI16(resp_buf, &wpos, 0);
                if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
            }
            if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        self.persistOffsets();
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // OffsetFetch (key 9)
    // ---------------------------------------------------------------
    fn handleOffsetFetch(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 6;
        const group_id = if (flexible)
            ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
        else
            ((ser.readString(request_bytes, &pos) catch return null) orelse "");

        const num_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));

        var resp_buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 3) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_topics) else ser.writeArrayLen(resp_buf, &wpos, num_topics);

        for (0..num_topics) |_| {
            const topic_name = if (flexible)
                ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
            else
                ((ser.readString(request_bytes, &pos) catch return null) orelse "");
            if (flexible) ser.writeCompactString(resp_buf, &wpos, topic_name) else ser.writeString(resp_buf, &wpos, topic_name);

            const num_partitions = if (flexible)
                (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
            else
                @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
            if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_partitions) else ser.writeArrayLen(resp_buf, &wpos, num_partitions);

            for (0..num_partitions) |_| {
                const partition_id = ser.readI32(request_bytes, &pos);
                const committed = self.groups.fetchOffset(group_id, topic_name, partition_id) catch null;
                ser.writeI32(resp_buf, &wpos, partition_id);
                ser.writeI64(resp_buf, &wpos, committed orelse -1);
                if (flexible) ser.writeCompactString(resp_buf, &wpos, null) else ser.writeString(resp_buf, &wpos, null); // metadata
                ser.writeI16(resp_buf, &wpos, 0);
                if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
            }
            if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        }
        if (api_version >= 2) ser.writeI16(resp_buf, &wpos, 0); // error_code (v2+)
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // CreateTopics (key 19)
    // ---------------------------------------------------------------
    fn handleCreateTopics(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 5;
        const readStr = if (flexible) &ser.readCompactString else &ser.readString;

        const num_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));

        var resp_buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 2) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_topics) else ser.writeArrayLen(resp_buf, &wpos, num_topics);

        for (0..num_topics) |_| {
            const topic_name = (readStr(request_bytes, &pos) catch return null) orelse "";
            const num_partitions = ser.readI32(request_bytes, &pos);
            const replication_factor = ser.readI16(request_bytes, &pos);

            // Skip assignments and configs arrays
            const num_assignments = if (flexible)
                (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0
            else
                @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
            for (0..num_assignments) |_| {
                _ = ser.readI32(request_bytes, &pos);
                const nbi = if (flexible) (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0 else @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
                pos += nbi * 4;
                if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch {};
            }
            const num_configs = if (flexible)
                (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0
            else
                @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
            for (0..num_configs) |_| {
                _ = readStr(request_bytes, &pos) catch break;
                _ = readStr(request_bytes, &pos) catch break;
                if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch {};
            }
            if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch {};

            const actual_partitions: i32 = if (num_partitions <= 0) self.default_num_partitions else num_partitions;
            const actual_rf: i16 = if (replication_factor <= 0) self.default_replication_factor else replication_factor;

            var error_code: i16 = 0;
            if (self.topics.contains(topic_name)) {
                error_code = 36;
            } else {
                const name_copy = self.allocator.dupe(u8, topic_name) catch return null;
                const key_copy = self.allocator.dupe(u8, topic_name) catch {
                    self.allocator.free(name_copy);
                    return null;
                };
                self.topics.put(key_copy, .{
                    .name = name_copy,
                    .num_partitions = actual_partitions,
                    .replication_factor = actual_rf,
                    .topic_id = TopicInfo.generateTopicId(),
                }) catch return null;
                for (0..@intCast(actual_partitions)) |pi| self.store.ensurePartition(topic_name, @intCast(pi)) catch {};
                log.info("Created topic '{s}' ({d} partitions)", .{ topic_name, actual_partitions });
                self.persistTopics();
                self.persistObjectManagerSnapshot();
            }

            if (flexible) ser.writeCompactString(resp_buf, &wpos, topic_name) else ser.writeString(resp_buf, &wpos, topic_name);
            if (api_version >= 7) {
                // v7+: topic_id UUID
                if (self.topics.get(topic_name)) |info| {
                    ser.writeUuid(resp_buf, &wpos, info.topic_id);
                } else {
                    ser.writeUuid(resp_buf, &wpos, .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
                }
            }
            ser.writeI16(resp_buf, &wpos, error_code);
            if (api_version >= 1) {
                if (flexible) ser.writeCompactString(resp_buf, &wpos, null) else ser.writeString(resp_buf, &wpos, null); // error_message
            }
            if (api_version >= 5) {
                ser.writeI32(resp_buf, &wpos, actual_partitions); // num_partitions
                ser.writeI16(resp_buf, &wpos, actual_rf); // replication_factor
                if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, 0) else ser.writeArrayLen(resp_buf, &wpos, 0); // configs
            }
            if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // SyncGroup (key 14)
    // ---------------------------------------------------------------
    fn handleSyncGroup(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 4;
        const readStr = if (flexible) &ser.readCompactString else &ser.readString;
        const group_id = (readStr(request_bytes, &pos) catch return null) orelse "";
        const generation_id = ser.readI32(request_bytes, &pos);
        const member_id = (readStr(request_bytes, &pos) catch return null) orelse "";
        if (api_version >= 3) _ = readStr(request_bytes, &pos) catch null; // group_instance_id
        if (api_version >= 5) _ = readStr(request_bytes, &pos) catch null; // protocol_type
        if (api_version >= 5) _ = readStr(request_bytes, &pos) catch null; // protocol_name

        const GroupCoord = @import("group_coordinator.zig").GroupCoordinator;
        const num_assignments = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));
        var assignments_buf: [64]GroupCoord.MemberAssignment = undefined;
        var assign_count: usize = 0;

        for (0..num_assignments) |_| {
            if (assign_count >= 64) break;
            const a_member = (readStr(request_bytes, &pos) catch break) orelse "";
            const a_data = if (flexible) (ser.readCompactBytes(request_bytes, &pos) catch break) orelse "" else (ser.readBytes(request_bytes, &pos) catch break) orelse "";
            if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch {};
            assignments_buf[assign_count] = .{ .member_id = a_member, .assignment = a_data };
            assign_count += 1;
        }

        const assignments: ?[]const GroupCoord.MemberAssignment = if (assign_count > 0) assignments_buf[0..assign_count] else null;
        const result = self.groups.syncGroup(group_id, member_id, generation_id, assignments) catch blk: {
            break :blk GroupCoord.SyncGroupResult{ .error_code = 16, .assignment = null };
        };

        var resp_buf = self.allocator.alloc(u8, 1024) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 1) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(resp_buf, &wpos, result.error_code);
        if (api_version >= 5) {
            if (flexible) ser.writeCompactString(resp_buf, &wpos, "consumer") else ser.writeString(resp_buf, &wpos, "consumer"); // protocol_type
            if (flexible) ser.writeCompactString(resp_buf, &wpos, "range") else ser.writeString(resp_buf, &wpos, "range"); // protocol_name
        }
        if (flexible) ser.writeCompactBytes(resp_buf, &wpos, result.assignment) else ser.writeBytesBuf(resp_buf, &wpos, result.assignment);
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DescribeGroups (key 15)
    // ---------------------------------------------------------------
    fn handleDescribeGroups(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        // Parse requested group IDs
        const num_groups = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var resp_buf = self.allocator.alloc(u8, 8192) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);

        if (num_groups > 0) {
            // Describe specific groups
            ser.writeArrayLen(resp_buf, &wpos, num_groups);
            for (0..num_groups) |_| {
                const gid = (ser.readString(request_bytes, &pos) catch break) orelse "";
                if (self.groups.groups.getPtr(gid)) |group| {
                    self.writeGroupDescription(resp_buf, &wpos, group);
                } else {
                    // Group not found
                    ser.writeI16(resp_buf, &wpos, 69); // GROUP_ID_NOT_FOUND
                    ser.writeString(resp_buf, &wpos, gid);
                    ser.writeString(resp_buf, &wpos, "Dead");
                    ser.writeString(resp_buf, &wpos, "");
                    ser.writeString(resp_buf, &wpos, "");
                    ser.writeArrayLen(resp_buf, &wpos, 0);
                }
            }
        } else {
            // Return all groups
            const group_count = self.groups.groupCount();
            ser.writeArrayLen(resp_buf, &wpos, group_count);
            var git = self.groups.groups.iterator();
            while (git.next()) |entry| {
                self.writeGroupDescription(resp_buf, &wpos, entry.value_ptr);
            }
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    fn writeGroupDescription(self: *Broker, buf: []u8, wpos: *usize, group: *const @import("group_coordinator.zig").ConsumerGroup) void {
        _ = self;
        ser.writeI16(buf, wpos, 0); // error_code
        ser.writeString(buf, wpos, group.group_id);
        ser.writeString(buf, wpos, switch (group.state) {
            .empty => "Empty",
            .preparing_rebalance => "PreparingRebalance",
            .completing_rebalance => "CompletingRebalance",
            .stable => "Stable",
            .dead => "Dead",
        });
        ser.writeString(buf, wpos, "consumer"); // protocol_type
        ser.writeString(buf, wpos, "range"); // protocol (assignment strategy)
        ser.writeArrayLen(buf, wpos, group.memberCount());

        var mit = group.members.iterator();
        while (mit.next()) |mentry| {
            const member = mentry.value_ptr;
            ser.writeString(buf, wpos, member.member_id);
            ser.writeString(buf, wpos, "zmq-client"); // client_id
            ser.writeString(buf, wpos, "/127.0.0.1"); // client_host
            ser.writeBytesBuf(buf, wpos, null); // metadata
            ser.writeBytesBuf(buf, wpos, member.assignment); // assignment
        }
    }

    // ---------------------------------------------------------------
    // ListGroups (key 16)
    // ---------------------------------------------------------------
    fn handleListGroups(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var resp_buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        ser.writeI16(resp_buf, &wpos, 0); // error_code

        const group_count = self.groups.groupCount();
        ser.writeArrayLen(resp_buf, &wpos, group_count);

        var git = self.groups.groups.iterator();
        while (git.next()) |entry| {
            ser.writeString(resp_buf, &wpos, entry.value_ptr.group_id);
            ser.writeString(resp_buf, &wpos, "consumer"); // protocol_type
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DeleteTopics (key 20)
    // ---------------------------------------------------------------
    fn handleDeleteTopics(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 4;
        const num_topics = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
        else
            @as(usize, @intCast(@max(ser.readI32(request_bytes, &pos), 0)));

        var resp_buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        if (api_version >= 1) ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        if (flexible) ser.writeCompactArrayLen(resp_buf, &wpos, num_topics) else ser.writeArrayLen(resp_buf, &wpos, num_topics);

        for (0..num_topics) |_| {
            const topic_name = if (flexible)
                ((ser.readCompactString(request_bytes, &pos) catch return null) orelse "")
            else
                ((ser.readString(request_bytes, &pos) catch return null) orelse "");

            var error_code: i16 = 0;

            if (std.mem.startsWith(u8, topic_name, "__")) {
                error_code = 73; // TOPIC_DELETION_DISABLED
            } else if (self.topics.fetchRemove(topic_name)) |removed| {
                const info = removed.value;
                for (0..@as(usize, @intCast(info.num_partitions))) |pi| {
                    const stream_id = PartitionStore.hashPartitionKey(topic_name, @intCast(pi));
                    self.object_manager.deleteStream(stream_id) catch {};
                    const pkey = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ topic_name, pi }) catch continue;
                    defer self.allocator.free(pkey);
                    if (self.store.partitions.fetchRemove(pkey)) |se| {
                        self.allocator.free(se.value.topic);
                        self.allocator.free(se.key);
                    }
                }
                self.allocator.free(info.name);
                self.allocator.free(removed.key);
                log.info("Deleted topic '{s}' ({d} partitions)", .{ topic_name, info.num_partitions });
                self.persistTopics();
                self.persistObjectManagerSnapshot();
            } else {
                error_code = 3;
            }

            if (flexible) ser.writeCompactString(resp_buf, &wpos, topic_name) else ser.writeString(resp_buf, &wpos, topic_name);
            if (api_version >= 6) {
                // v6+: topic_id
                ser.writeUuid(resp_buf, &wpos, .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
            }
            ser.writeI16(resp_buf, &wpos, error_code);
            if (api_version >= 5) {
                if (flexible) ser.writeCompactString(resp_buf, &wpos, null) else ser.writeString(resp_buf, &wpos, null); // error_message
            }
            if (flexible) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // InitProducerId (key 22)
    // ---------------------------------------------------------------
    fn handleInitProducerId(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        // Parse transactional_id (nullable string)
        const transactional_id = ser.readString(request_bytes, &pos) catch null;
        const timeout_ms = ser.readI32(request_bytes, &pos);
        _ = timeout_ms;

        // Allocate a producer ID for idempotent/transactional producers
        const result = self.txn_coordinator.initProducerId(transactional_id) catch return null;

        var resp_buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(resp_buf, &wpos, result.error_code); // error_code
        ser.writeI64(resp_buf, &wpos, result.producer_id); // producer_id
        ser.writeI16(resp_buf, &wpos, result.producer_epoch); // producer_epoch
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DescribeConfigs (key 32)
    // ---------------------------------------------------------------
    fn handleDescribeConfigs(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const num_resources = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var resp_buf = self.allocator.alloc(u8, 8192) catch return null;
        var wpos: usize = 0;
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        resp_header.serialize(resp_buf, &wpos, resp_header_version);
        ser.writeI32(resp_buf, &wpos, 0); // throttle_time_ms

        if (num_resources == 0) {
            ser.writeArrayLen(resp_buf, &wpos, 0);
        } else {
            ser.writeArrayLen(resp_buf, &wpos, num_resources);

            for (0..num_resources) |_| {
                const resource_type = ser.readI8(request_bytes, &pos);
                const resource_name = (ser.readString(request_bytes, &pos) catch break) orelse "";
                // Skip config_keys array if present
                const num_keys = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
                for (0..num_keys) |_| {
                    _ = ser.readString(request_bytes, &pos) catch break;
                }

                ser.writeI16(resp_buf, &wpos, 0); // error_code
                ser.writeString(resp_buf, &wpos, null); // error_message
                ser.writeI8(resp_buf, &wpos, resource_type);
                ser.writeString(resp_buf, &wpos, resource_name);

                if (resource_type == 2) {
                    // TOPIC — return per-topic configuration
                    if (self.topics.get(resource_name)) |info| {
                        const tc = info.config;
                        const topic_configs = [_]struct { name: []const u8, value: []const u8 }{
                            .{ .name = "retention.ms", .value = "604800000" },
                            .{ .name = "retention.bytes", .value = "-1" },
                            .{ .name = "max.message.bytes", .value = "1048576" },
                            .{ .name = "segment.bytes", .value = "1073741824" },
                            .{ .name = "cleanup.policy", .value = tc.cleanup_policy },
                            .{ .name = "compression.type", .value = tc.compression_type },
                            .{ .name = "min.insync.replicas", .value = "1" },
                        };
                        ser.writeArrayLen(resp_buf, &wpos, topic_configs.len);
                        for (topic_configs) |cfg| {
                            ser.writeString(resp_buf, &wpos, cfg.name);
                            ser.writeString(resp_buf, &wpos, cfg.value);
                            ser.writeBool(resp_buf, &wpos, false);
                            ser.writeBool(resp_buf, &wpos, true); // is_default
                            ser.writeBool(resp_buf, &wpos, false);
                        }
                    } else {
                        ser.writeArrayLen(resp_buf, &wpos, 0);
                    }
                } else {
                    // BROKER — return broker-level configuration
                    const broker_configs = [_]struct { name: []const u8, value: []const u8 }{
                        .{ .name = "broker.id", .value = "0" },
                        .{ .name = "log.retention.hours", .value = "168" },
                        .{ .name = "num.partitions", .value = "1" },
                        .{ .name = "auto.create.topics.enable", .value = if (self.auto_create_topics) "true" else "false" },
                        .{ .name = "min.insync.replicas", .value = "1" },
                        .{ .name = "message.max.bytes", .value = "1048576" },
                        .{ .name = "log.segment.bytes", .value = "1073741824" },
                        .{ .name = "log.cleanup.policy", .value = "delete" },
                        .{ .name = "compression.type", .value = "producer" },
                    };
                    ser.writeArrayLen(resp_buf, &wpos, broker_configs.len);
                    for (broker_configs) |cfg| {
                        ser.writeString(resp_buf, &wpos, cfg.name);
                        ser.writeString(resp_buf, &wpos, cfg.value);
                        ser.writeBool(resp_buf, &wpos, false);
                        ser.writeBool(resp_buf, &wpos, true);
                        ser.writeBool(resp_buf, &wpos, false);
                    }
                }
            }
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(resp_buf, &wpos);
        return (self.allocator.realloc(resp_buf, wpos) catch resp_buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DeleteRecords (key 21)
    // ---------------------------------------------------------------
    fn handleDeleteRecords(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const num_topics = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, num_topics);

        var mutated = false;
        for (0..num_topics) |_| {
            const topic = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const num_parts = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

            ser.writeString(buf, &wpos, topic);
            ser.writeArrayLen(buf, &wpos, num_parts);

            for (0..num_parts) |_| {
                const partition = ser.readI32(request_bytes, &pos);
                const delete_offset = ser.readI64(request_bytes, &pos);

                // Update log_start_offset for the partition
                const pkey = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ topic, partition }) catch continue;
                defer self.allocator.free(pkey);

                var low_watermark: i64 = 0;
                if (self.store.partitions.getPtr(pkey)) |state| {
                    if (delete_offset > 0 and @as(u64, @intCast(delete_offset)) > state.log_start_offset) {
                        state.log_start_offset = @intCast(delete_offset);
                        const stream_id = PartitionStore.hashPartitionKey(topic, partition);
                        self.object_manager.trimStream(stream_id, state.log_start_offset) catch {};
                        low_watermark = delete_offset;
                        mutated = true;
                    } else {
                        low_watermark = @intCast(state.log_start_offset);
                    }
                }

                ser.writeI32(buf, &wpos, partition);
                ser.writeI64(buf, &wpos, low_watermark); // low_watermark
                ser.writeI16(buf, &wpos, 0); // error_code
            }
        }
        if (mutated) {
            self.persistPartitionStates();
            self.persistObjectManagerSnapshot();
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // AddPartitionsToTxn (key 24)
    // ---------------------------------------------------------------
    fn handleAddPartitionsToTxn(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const transactional_id = (ser.readString(request_bytes, &pos) catch return null) orelse "";
        _ = transactional_id;
        const producer_id = ser.readI64(request_bytes, &pos);
        const producer_epoch = ser.readI16(request_bytes, &pos);

        // Read topics array
        const topics_len = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
        var topic_errors: [32]struct { topic: []const u8, partitions: [32]struct { partition: i32, error_code: i16 }, num_partitions: usize } = undefined;
        var num_topics: usize = 0;

        for (0..topics_len) |_| {
            if (num_topics >= 32) break;
            const topic = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const parts_len = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

            var te = &topic_errors[num_topics];
            te.topic = topic;
            te.num_partitions = 0;

            for (0..parts_len) |_| {
                if (te.num_partitions >= 32) break;
                const partition = ser.readI32(request_bytes, &pos);
                const err = self.txn_coordinator.addPartitionsToTxn(producer_id, producer_epoch, topic, partition) catch 48;
                te.partitions[te.num_partitions] = .{ .partition = partition, .error_code = err };
                te.num_partitions += 1;
            }
            num_topics += 1;
        }

        var buf = self.allocator.alloc(u8, 2048) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, num_topics);
        for (0..num_topics) |i| {
            const te = &topic_errors[i];
            ser.writeString(buf, &wpos, te.topic);
            ser.writeArrayLen(buf, &wpos, te.num_partitions);
            for (0..te.num_partitions) |j| {
                ser.writeI32(buf, &wpos, te.partitions[j].partition);
                ser.writeI16(buf, &wpos, te.partitions[j].error_code);
            }
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // EndTxn (key 26) — Write control batches to partition store
    // ---------------------------------------------------------------
    fn handleEndTxn(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const transactional_id = (ser.readString(request_bytes, &pos) catch return null) orelse "";
        _ = transactional_id;
        const producer_id = ser.readI64(request_bytes, &pos);
        const producer_epoch = ser.readI16(request_bytes, &pos);
        const committed = ser.readBool(request_bytes, &pos) catch false;

        // Before completing the txn, get the partition list so we can write control batches
        const control_type: @import("txn_coordinator.zig").TransactionCoordinator.ControlRecordType = if (committed) .commit else .abort;
        var partition_state_dirty = false;
        if (self.txn_coordinator.getPartitions(producer_id)) |partitions| {
            // Write control batch to each partition in the transaction
            for (partitions) |tp| {
                const pkey = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ tp.topic, tp.partition }) catch continue;
                defer self.allocator.free(pkey);
                const base_off: i64 = if (self.store.partitions.get(pkey)) |state|
                    @intCast(state.next_offset)
                else
                    0;

                const control_batch = self.txn_coordinator.buildControlBatch(
                    producer_id,
                    producer_epoch,
                    control_type,
                    base_off,
                ) catch continue;
                defer self.allocator.free(control_batch);

                // Write the control batch to the partition store
                _ = self.store.produce(tp.topic, tp.partition, control_batch) catch |err| {
                    log.warn("Failed to write control batch for {s}-{d}: {}", .{ tp.topic, tp.partition, err });
                    continue;
                };
                partition_state_dirty = true;
            }

            // Clear first_unstable_txn_offset on each partition so LSO advances.
            // NOTE: AutoMQ/Kafka advances LSO after control batch markers are written.
            // Without this, READ_COMMITTED consumers would never see committed data.
            for (partitions) |tp| {
                const pkey2 = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ tp.topic, tp.partition }) catch continue;
                defer self.allocator.free(pkey2);
                if (self.store.partitions.getPtr(pkey2)) |state| {
                    state.first_unstable_txn_offset = null;
                    // Recompute LSO = HW (no unstable transactions on this partition now)
                    state.last_stable_offset = state.high_watermark;
                    partition_state_dirty = true;
                }
            }
        }
        if (partition_state_dirty) {
            self.persistPartitionStates();
            self.persistObjectManagerSnapshot();
        }

        const error_code = self.txn_coordinator.endTxnComplete(producer_id, producer_epoch, committed);

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, error_code);
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // AlterConfigs (key 33)
    // ---------------------------------------------------------------
    fn handleAlterConfigs(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const num_resources = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var results: [32]struct { resource_type: i8, resource_name: []const u8, error_code: i16 } = undefined;
        var count: usize = 0;

        for (0..num_resources) |_| {
            if (count >= 32) break;
            const resource_type = ser.readI8(request_bytes, &pos);
            const resource_name = (ser.readString(request_bytes, &pos) catch break) orelse "";

            // Parse config entries
            const num_configs = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
            for (0..num_configs) |_| {
                const config_name = (ser.readString(request_bytes, &pos) catch break) orelse "";
                const config_value = (ser.readString(request_bytes, &pos) catch break) orelse "";
                _ = config_name;
                _ = config_value;
                // In production: apply config changes to broker/topic
            }

            results[count] = .{
                .resource_type = resource_type,
                .resource_name = resource_name,
                .error_code = 0,
            };
            count += 1;
        }

        var buf = self.allocator.alloc(u8, 1024) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, count);
        for (0..count) |i| {
            ser.writeI16(buf, &wpos, results[i].error_code);
            ser.writeString(buf, &wpos, null); // error_message
            ser.writeI8(buf, &wpos, results[i].resource_type);
            ser.writeString(buf, &wpos, results[i].resource_name);
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // CreatePartitions (key 37)
    // ---------------------------------------------------------------
    fn handleCreatePartitions(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const num_topics = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var results: [64]struct { name: []const u8, error_code: i16 } = undefined;
        var count: usize = 0;
        var mutated = false;

        for (0..num_topics) |_| {
            if (count >= 64) break;
            const topic_name = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const new_total_count = ser.readI32(request_bytes, &pos);
            // Skip assignments array
            const num_assigns = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
            for (0..num_assigns) |_| {
                const num_broker_ids = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
                pos += num_broker_ids * 4;
            }

            // Update topic partition count
            if (self.topics.getPtr(topic_name)) |info| {
                if (new_total_count > info.num_partitions) {
                    const old_total_count = info.num_partitions;
                    info.num_partitions = new_total_count;
                    // Ensure new partitions exist in store
                    for (@as(usize, @intCast(old_total_count))..@as(usize, @intCast(new_total_count))) |pi| {
                        self.store.ensurePartition(topic_name, @intCast(pi)) catch {};
                    }
                    results[count] = .{ .name = topic_name, .error_code = 0 };
                    mutated = true;
                } else {
                    results[count] = .{ .name = topic_name, .error_code = 37 }; // INVALID_PARTITIONS
                }
            } else {
                results[count] = .{ .name = topic_name, .error_code = 3 }; // UNKNOWN_TOPIC_OR_PARTITION
            }
            count += 1;
        }
        if (mutated) {
            self.persistTopics();
            self.persistObjectManagerSnapshot();
        }

        var buf = self.allocator.alloc(u8, 2048) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, count);
        for (0..count) |i| {
            ser.writeString(buf, &wpos, results[i].name);
            ser.writeI16(buf, &wpos, results[i].error_code);
            ser.writeString(buf, &wpos, null); // error_message
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // LeaderAndIsr (key 4) — inter-broker RPC no-op
    // ZMQ is single-node with RF=1, so this is a protocol-compatible no-op.
    // ---------------------------------------------------------------
    fn handleLeaderAndIsr(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        _ = request_bytes;
        _ = body_start;
        var buf = self.allocator.alloc(u8, 128) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code = NONE
        ser.writeArrayLen(buf, &wpos, 0); // partition_errors (empty)
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // StopReplica (key 5) — inter-broker RPC no-op
    // ---------------------------------------------------------------
    fn handleStopReplica(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        _ = request_bytes;
        _ = body_start;
        var buf = self.allocator.alloc(u8, 128) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code = NONE
        ser.writeArrayLen(buf, &wpos, 0); // partition_errors (empty)
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // UpdateMetadata (key 6) — inter-broker RPC no-op
    // ---------------------------------------------------------------
    fn handleUpdateMetadata(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        _ = request_bytes;
        _ = body_start;
        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code = NONE
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // ControlledShutdown (key 7) — inter-broker RPC no-op
    // ---------------------------------------------------------------
    fn handleControlledShutdown(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code = NONE
        ser.writeArrayLen(buf, &wpos, 0); // remaining_partitions (empty)
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // AddOffsetsToTxn (key 25) — register __consumer_offsets in txn
    // Parses group_id, computes the __consumer_offsets partition, and adds it
    // to the transaction via txn_coordinator.addPartitionsToTxn.
    // ---------------------------------------------------------------
    fn handleAddOffsetsToTxn(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;

        // Parse: transactional_id, producer_id, producer_epoch, group_id
        const transactional_id = (ser.readString(request_bytes, &pos) catch return null) orelse "";
        _ = transactional_id;
        const producer_id = ser.readI64(request_bytes, &pos);
        const producer_epoch = ser.readI16(request_bytes, &pos);
        const group_id = (ser.readString(request_bytes, &pos) catch return null) orelse "";

        // Compute partition = hash(group_id) % 50 for __consumer_offsets
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(group_id);
        const target_partition: i32 = @intCast(hasher.final() % 50);

        // Add __consumer_offsets partition to the transaction
        const error_code = self.txn_coordinator.addPartitionsToTxn(
            producer_id,
            producer_epoch,
            "__consumer_offsets",
            target_partition,
        ) catch 48; // INVALID_PRODUCER_ID_MAPPING on error

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, error_code);
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DeleteGroups (key 42)
    // ---------------------------------------------------------------
    fn handleDeleteGroups(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const flexible = api_version >= 2;
        const num_groups = if (flexible)
            (ser.readCompactArrayLen(request_bytes, &pos) catch return null) orelse 0
        else
            (ser.readArrayLen(request_bytes, &pos) catch return null) orelse 0;

        var results = std.array_list.Managed(DeleteGroupsResponse.DeletableGroupResult).init(self.allocator);
        defer results.deinit();

        for (0..num_groups) |_| {
            const group_id = if (flexible)
                (ser.readCompactString(request_bytes, &pos) catch return null) orelse ""
            else
                (ser.readString(request_bytes, &pos) catch return null) orelse "";

            const error_code = self.groups.deleteGroup(group_id);
            results.append(.{ .group_id = group_id, .error_code = error_code }) catch return null;
        }

        if (flexible) ser.skipTaggedFields(request_bytes, &pos) catch return null;

        const resp_body = DeleteGroupsResponse{
            .throttle_time_ms = 0,
            .results = results.items,
        };
        const body_version: i16 = @min(api_version, 2);
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const total_size = resp_header.calcSize(resp_header_version) + resp_body.calcSize(body_version);
        const buf = self.allocator.alloc(u8, total_size) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        resp_body.serialize(buf, &wpos, body_version);
        return buf[0..wpos];
    }

    // ---------------------------------------------------------------
    // AutoMQ extension APIs (keys 501-519, 600-602)
    // ---------------------------------------------------------------
    fn handleCreateStreams(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.create_streams_request.CreateStreamsRequest;
        const Resp = generated.create_streams_response.CreateStreamsResponse;
        const ItemResp = Resp.CreateStreamResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.create_stream_requests.len) catch return null;
        var mutated = false;
        for (req.create_stream_requests, 0..) |item, i| {
            const owner_node = if (item.node_id != 0) item.node_id else if (req.node_id != 0) req.node_id else self.node_id;
            const stream = self.object_manager.createStream(owner_node) catch |err| {
                responses[i] = .{ .error_code = streamErrorCode(err), .stream_id = -1 };
                continue;
            };
            responses[i] = .{ .error_code = 0, .stream_id = u64ToI64(stream.stream_id) };
            mutated = true;
        }
        if (mutated) self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .create_stream_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleOpenStreams(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.open_streams_request.OpenStreamsRequest;
        const Resp = generated.open_streams_response.OpenStreamsResponse;
        const ItemResp = Resp.OpenStreamResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.open_stream_requests.len) catch return null;
        var mutated = false;
        for (req.open_stream_requests, 0..) |item, i| {
            const stream_id = i64ToU64(item.stream_id) orelse {
                responses[i] = .{ .error_code = errorCode(.invalid_request), .start_offset = -1, .next_offset = -1 };
                continue;
            };
            const requested_epoch: u64 = @intCast(@max(item.stream_epoch, 0));
            self.object_manager.openStream(stream_id, requested_epoch) catch |err| {
                responses[i] = .{ .error_code = streamErrorCode(err), .start_offset = -1, .next_offset = -1 };
                continue;
            };
            const stream = self.object_manager.getStream(stream_id).?;
            responses[i] = .{
                .error_code = 0,
                .start_offset = u64ToI64(stream.start_offset),
                .next_offset = u64ToI64(stream.end_offset),
            };
            mutated = true;
        }
        if (mutated) self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .open_stream_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleCloseStreams(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.close_streams_request.CloseStreamsRequest;
        const Resp = generated.close_streams_response.CloseStreamsResponse;
        const ItemResp = Resp.CloseStreamResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.close_stream_requests.len) catch return null;
        var mutated = false;
        for (req.close_stream_requests, 0..) |item, i| {
            const stream_id = i64ToU64(item.stream_id) orelse {
                responses[i] = .{ .error_code = errorCode(.invalid_request) };
                continue;
            };
            const stream = self.object_manager.getStream(stream_id) orelse {
                responses[i] = .{ .error_code = errorCode(.resource_not_found) };
                continue;
            };
            if (item.stream_epoch >= 0 and @as(u64, @intCast(item.stream_epoch)) != stream.epoch) {
                responses[i] = .{ .error_code = errorCode(.fenced_leader_epoch) };
                continue;
            }
            self.object_manager.closeStream(stream_id) catch |err| {
                responses[i] = .{ .error_code = streamErrorCode(err) };
                continue;
            };
            responses[i] = .{ .error_code = 0 };
            mutated = true;
        }
        if (mutated) self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .close_stream_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleDeleteStreams(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.delete_streams_request.DeleteStreamsRequest;
        const Resp = generated.delete_streams_response.DeleteStreamsResponse;
        const ItemResp = Resp.DeleteStreamResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.delete_stream_requests.len) catch return null;
        var mutated = false;
        for (req.delete_stream_requests, 0..) |item, i| {
            const stream_id = i64ToU64(item.stream_id) orelse {
                responses[i] = .{ .error_code = errorCode(.invalid_request) };
                continue;
            };
            self.object_manager.deleteStream(stream_id) catch |err| {
                responses[i] = .{ .error_code = streamErrorCode(err) };
                continue;
            };
            responses[i] = .{ .error_code = 0 };
            mutated = true;
        }
        if (mutated) self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .delete_stream_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handlePrepareS3Object(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.prepare_s3_object_request.PrepareS3ObjectRequest;
        const Resp = generated.prepare_s3_object_response.PrepareS3ObjectResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const req = parseGeneratedRequest(Req, arena.allocator(), request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0, .first_s3_object_id = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        if (req.prepared_count <= 0) {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0, .first_s3_object_id = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var first_id: u64 = 0;
        var i: i32 = 0;
        while (i < req.prepared_count) : (i += 1) {
            const object_id = self.object_manager.prepareObject();
            if (i == 0) first_id = object_id;
        }
        self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .first_s3_object_id = u64ToI64(first_id) };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleCommitStreamSetObject(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.commit_stream_set_object_request.CommitStreamSetObjectRequest;
        const Resp = generated.commit_stream_set_object_response.CommitStreamSetObjectResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const object_id = i64ToU64(req.object_id) orelse {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        const object_size = i64ToU64(req.object_size) orelse {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        const order_id = i64ToU64(req.order_id) orelse object_id;

        const ranges = arena_alloc.alloc(storage.stream.StreamOffsetRange, req.object_stream_ranges.len) catch return null;
        for (req.object_stream_ranges, 0..) |range_req, i| {
            const stream_id = i64ToU64(range_req.stream_id) orelse {
                const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            const start_offset = i64ToU64(range_req.start_offset) orelse {
                const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            const end_offset = i64ToU64(range_req.end_offset) orelse {
                const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            if (start_offset > end_offset or self.object_manager.getStream(stream_id) == null) {
                const resp = Resp{ .error_code = errorCode(.resource_not_found), .throttle_time_ms = 0 };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            }
            ranges[i] = .{ .stream_id = stream_id, .start_offset = start_offset, .end_offset = end_offset };
        }

        const sso_key = self.makeStreamSetObjectKey(object_id, req.node_id) catch return null;
        defer self.allocator.free(sso_key);
        self.object_manager.commitStreamSetObject(object_id, req.node_id, order_id, ranges, sso_key, object_size) catch |err| {
            const resp = Resp{ .error_code = streamErrorCode(err), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        var mutated = true;

        for (req.stream_objects) |stream_object| {
            const so_object_id = i64ToU64(stream_object.object_id) orelse continue;
            const stream_id = i64ToU64(stream_object.stream_id) orelse continue;
            const start_offset = i64ToU64(stream_object.start_offset) orelse continue;
            const end_offset = i64ToU64(stream_object.end_offset) orelse continue;
            const so_size = i64ToU64(stream_object.object_size) orelse continue;
            if (start_offset > end_offset or self.object_manager.getStream(stream_id) == null) continue;
            const so_key = self.makeStreamObjectKey(so_object_id, stream_id, start_offset, end_offset) catch return null;
            defer self.allocator.free(so_key);
            self.object_manager.commitStreamObject(so_object_id, stream_id, start_offset, end_offset, so_key, so_size) catch {};
            mutated = true;
        }

        for (req.compacted_object_ids) |compacted_id| {
            if (i64ToU64(compacted_id)) |id| {
                self.object_manager.markDestroyed(id);
                mutated = true;
            }
        }
        if (mutated) self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .attributes = req.attributes };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleCommitStreamObject(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.commit_stream_object_request.CommitStreamObjectRequest;
        const Resp = generated.commit_stream_object_response.CommitStreamObjectResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const req = parseGeneratedRequest(Req, arena.allocator(), request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const object_id = i64ToU64(req.object_id) orelse {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        const object_size = i64ToU64(req.object_size) orelse {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        const stream_id = i64ToU64(req.stream_id) orelse {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        const start_offset = i64ToU64(req.start_offset) orelse {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        const end_offset = i64ToU64(req.end_offset) orelse {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        if (start_offset > end_offset or self.object_manager.getStream(stream_id) == null) {
            const resp = Resp{ .error_code = errorCode(.resource_not_found), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const key = self.makeStreamObjectKey(object_id, stream_id, start_offset, end_offset) catch return null;
        defer self.allocator.free(key);
        self.object_manager.commitStreamObject(object_id, stream_id, start_offset, end_offset, key, object_size) catch |err| {
            const resp = Resp{ .error_code = streamErrorCode(err), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        var mutated = true;

        if (api_version >= 1 and req.operations.len > 0) {
            const count = @min(req.source_object_ids.len, req.operations.len);
            for (0..count) |i| {
                if (req.operations[i] == 0) {
                    if (i64ToU64(req.source_object_ids[i])) |source_id| {
                        self.object_manager.markDestroyed(source_id);
                        mutated = true;
                    }
                }
            }
        } else {
            for (req.source_object_ids) |source_id_raw| {
                if (i64ToU64(source_id_raw)) |source_id| {
                    self.object_manager.markDestroyed(source_id);
                    mutated = true;
                }
            }
        }
        if (mutated) self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0 };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleGetOpeningStreams(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.get_opening_streams_request.GetOpeningStreamsRequest;
        const Resp = generated.get_opening_streams_response.GetOpeningStreamsResponse;
        const StreamMetadata = Resp.StreamMetadata;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        var streams = std.array_list.Managed(StreamMetadata).init(arena_alloc);
        var it = self.object_manager.streams.iterator();
        while (it.next()) |entry| {
            const stream = entry.value_ptr;
            if (stream.state != .opened) continue;
            if (req.node_id >= 0 and stream.node_id != req.node_id) continue;
            streams.append(.{
                .stream_id = u64ToI64(stream.stream_id),
                .epoch = u64ToI64(stream.epoch),
                .start_offset = u64ToI64(stream.start_offset),
                .end_offset = u64ToI64(stream.end_offset),
            }) catch return null;
        }

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .stream_metadata_list = streams.items };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleGetKVs(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.get_k_vs_request.GetKVsRequest;
        const Resp = generated.get_k_vs_response.GetKVsResponse;
        const ItemResp = Resp.GetKVResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.get_key_requests.len) catch return null;
        for (req.get_key_requests, 0..) |item, i| {
            const key = item.key orelse "";
            if (key.len == 0) {
                responses[i] = .{ .error_code = errorCode(.invalid_request), .value = null };
            } else if (self.auto_mq_kvs.get(key)) |value| {
                responses[i] = .{ .error_code = 0, .value = value };
            } else {
                responses[i] = .{ .error_code = errorCode(.resource_not_found), .value = null };
            }
        }

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .get_kv_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handlePutKVs(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.put_k_vs_request.PutKVsRequest;
        const Resp = generated.put_k_vs_response.PutKVsResponse;
        const ItemResp = Resp.PutKVResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.put_kv_requests.len) catch return null;
        var mutated = false;
        for (req.put_kv_requests, 0..) |item, i| {
            const key = item.key orelse "";
            const value = item.value orelse "";
            if (key.len == 0) {
                responses[i] = .{ .error_code = errorCode(.invalid_request), .value = null };
                continue;
            }
            if (self.auto_mq_kvs.getPtr(key)) |existing| {
                if (!item.overwrite) {
                    responses[i] = .{ .error_code = errorCode(.duplicate_resource), .value = existing.* };
                    continue;
                }
                const value_copy = self.allocator.dupe(u8, value) catch return null;
                self.allocator.free(existing.*);
                existing.* = value_copy;
                responses[i] = .{ .error_code = 0, .value = existing.* };
                mutated = true;
            } else {
                const key_copy = self.allocator.dupe(u8, key) catch return null;
                const value_copy = self.allocator.dupe(u8, value) catch {
                    self.allocator.free(key_copy);
                    return null;
                };
                self.auto_mq_kvs.put(key_copy, value_copy) catch {
                    self.allocator.free(key_copy);
                    self.allocator.free(value_copy);
                    return null;
                };
                responses[i] = .{ .error_code = 0, .value = value_copy };
                mutated = true;
            }
        }
        if (mutated) self.persistAutoMqMetadata();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .put_kv_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleDeleteKVs(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.delete_k_vs_request.DeleteKVsRequest;
        const Resp = generated.delete_k_vs_response.DeleteKVsResponse;
        const ItemResp = Resp.DeleteKVResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.delete_kv_requests.len) catch return null;
        var mutated = false;
        for (req.delete_kv_requests, 0..) |item, i| {
            const key = item.key orelse "";
            if (key.len == 0) {
                responses[i] = .{ .error_code = errorCode(.invalid_request), .value = null };
                continue;
            }
            if (self.auto_mq_kvs.fetchRemove(key)) |removed| {
                const response_value = arena_alloc.dupe(u8, removed.value) catch return null;
                self.allocator.free(removed.key);
                self.allocator.free(removed.value);
                responses[i] = .{ .error_code = 0, .value = response_value };
                mutated = true;
            } else {
                responses[i] = .{ .error_code = errorCode(.resource_not_found), .value = null };
            }
        }
        if (mutated) self.persistAutoMqMetadata();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .delete_kv_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleTrimStreams(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.trim_streams_request.TrimStreamsRequest;
        const Resp = generated.trim_streams_response.TrimStreamsResponse;
        const ItemResp = Resp.TrimStreamResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const responses = arena_alloc.alloc(ItemResp, req.trim_stream_requests.len) catch return null;
        var mutated = false;
        for (req.trim_stream_requests, 0..) |item, i| {
            const stream_id = i64ToU64(item.stream_id) orelse {
                responses[i] = .{ .error_code = errorCode(.invalid_request) };
                continue;
            };
            const new_start_offset = i64ToU64(item.new_start_offset) orelse {
                responses[i] = .{ .error_code = errorCode(.invalid_request) };
                continue;
            };
            self.object_manager.trimStream(stream_id, new_start_offset) catch |err| {
                responses[i] = .{ .error_code = streamErrorCode(err) };
                continue;
            };
            responses[i] = .{ .error_code = 0 };
            mutated = true;
        }
        if (mutated) self.persistObjectManagerSnapshot();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .trim_stream_responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleAutomqRegisterNode(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.automq_register_node_request.AutomqRegisterNodeRequest;
        const Resp = generated.automq_register_node_response.AutomqRegisterNodeResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const req = parseGeneratedRequest(Req, arena.allocator(), request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        if (req.node_id < 0 or req.node_id == std.math.maxInt(i32)) {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const wal_config = req.wal_config orelse "";
        const wal_config_copy = self.allocator.dupe(u8, wal_config) catch return null;

        if (self.auto_mq_nodes.getPtr(req.node_id)) |node| {
            node.deinit(self.allocator);
            node.* = .{ .node_epoch = req.node_epoch, .wal_config = wal_config_copy };
        } else {
            self.auto_mq_nodes.put(req.node_id, .{ .node_epoch = req.node_epoch, .wal_config = wal_config_copy }) catch {
                self.allocator.free(wal_config_copy);
                return null;
            };
        }
        if (req.node_id >= self.auto_mq_next_node_id) self.auto_mq_next_node_id = req.node_id + 1;
        self.persistAutoMqMetadata();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0 };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleAutomqGetNodes(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.automq_get_nodes_request.AutomqGetNodesRequest;
        const Resp = generated.automq_get_nodes_response.AutomqGetNodesResponse;
        const NodeMetadata = Resp.NodeMetadata;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        var nodes = std.array_list.Managed(NodeMetadata).init(arena_alloc);
        const include_self_synthetic = self.auto_mq_nodes.get(self.node_id) == null;

        if (req.node_ids.len == 0) {
            var it = self.auto_mq_nodes.iterator();
            while (it.next()) |entry| {
                nodes.append(.{
                    .node_id = entry.key_ptr.*,
                    .node_epoch = entry.value_ptr.node_epoch,
                    .wal_config = entry.value_ptr.wal_config,
                    .state = entry.value_ptr.state,
                    .has_opening_streams = self.hasOpeningStreamsForNode(entry.key_ptr.*),
                }) catch return null;
            }
            if (include_self_synthetic) {
                nodes.append(.{
                    .node_id = self.node_id,
                    .node_epoch = 0,
                    .wal_config = "local",
                    .state = "ACTIVE",
                    .has_opening_streams = self.hasOpeningStreamsForNode(self.node_id),
                }) catch return null;
            }
        } else {
            for (req.node_ids) |node_id| {
                if (self.auto_mq_nodes.get(node_id)) |node| {
                    nodes.append(.{
                        .node_id = node_id,
                        .node_epoch = node.node_epoch,
                        .wal_config = node.wal_config,
                        .state = node.state,
                        .has_opening_streams = self.hasOpeningStreamsForNode(node_id),
                    }) catch return null;
                } else if (node_id == self.node_id) {
                    nodes.append(.{
                        .node_id = self.node_id,
                        .node_epoch = 0,
                        .wal_config = "local",
                        .state = "ACTIVE",
                        .has_opening_streams = self.hasOpeningStreamsForNode(self.node_id),
                    }) catch return null;
                }
            }
        }

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .nodes = nodes.items };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleAutomqZoneRouter(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.automq_zone_router_request.AutomqZoneRouterRequest;
        const Resp = generated.automq_zone_router_response.AutomqZoneRouterResponse;
        const ItemResp = Resp.Response;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        var mutated = false;
        if (req.metadata) |metadata| {
            const metadata_copy = self.allocator.dupe(u8, metadata) catch return null;
            if (self.auto_mq_zone_router_metadata) |old| self.allocator.free(old);
            self.auto_mq_zone_router_metadata = metadata_copy;
            mutated = true;
        }
        if (api_version >= 1 and req.route_epoch > self.auto_mq_zone_router_epoch) {
            self.auto_mq_zone_router_epoch = req.route_epoch;
            mutated = true;
        }
        if (mutated) self.persistAutoMqMetadata();

        const default_route = std.fmt.allocPrint(arena_alloc, "{{\"node_id\":{d},\"epoch\":{d}}}", .{ self.node_id, self.auto_mq_zone_router_epoch }) catch return null;
        const responses = arena_alloc.alloc(ItemResp, 1) catch return null;
        responses[0] = .{ .data = self.auto_mq_zone_router_metadata orelse default_route };

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .responses = responses };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleAutomqGetPartitionSnapshot(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.automq_get_partition_snapshot_request.AutomqGetPartitionSnapshotRequest;
        const Resp = generated.automq_get_partition_snapshot_response.AutomqGetPartitionSnapshotResponse;
        const Topic = Resp.Topic;
        const PartitionSnapshot = Topic.PartitionSnapshot;
        const StreamMetadata = PartitionSnapshot.StreamMetadata;
        const LogOffsetMetadata = generated.automq_get_partition_snapshot_response.LogOffsetMetadata;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        var topics = std.array_list.Managed(Topic).init(arena_alloc);
        var topic_it = self.topics.iterator();
        while (topic_it.next()) |entry| {
            const info = entry.value_ptr;
            const partitions = arena_alloc.alloc(PartitionSnapshot, @intCast(@max(info.num_partitions, 0))) catch return null;
            var pi: i32 = 0;
            while (pi < info.num_partitions) : (pi += 1) {
                const stream_id = PartitionStore.hashPartitionKey(info.name, pi);
                const stream_metadata = arena_alloc.alloc(StreamMetadata, 1) catch return null;
                const partition_info = self.store.getPartitionInfo(info.name, pi);
                const end_offset = if (partition_info) |p| p.next_offset else 0;
                stream_metadata[0] = .{ .stream_id = u64ToI64(stream_id), .end_offset = end_offset };
                partitions[@intCast(pi)] = .{
                    .partition_index = pi,
                    .leader_epoch = if (self.raft_state) |rs| rs.current_epoch else self.cached_leader_epoch,
                    .operation = 0,
                    .log_end_offset = LogOffsetMetadata{ .message_offset = end_offset, .relative_position_in_segment = 0 },
                    .stream_metadata = stream_metadata,
                };
            }
            topics.append(.{ .topic_id = info.topic_id, .partitions = partitions }) catch return null;
        }

        const next_epoch = req.session_epoch + 1;
        const resp = Resp{
            .error_code = 0,
            .throttle_time_ms = 0,
            .session_id = req.session_id,
            .session_epoch = next_epoch,
            .topics = topics.items,
            .confirm_wal_end_offset = null,
            .confirm_wal_config = null,
            .confirm_wal_delta_data = null,
        };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleUpdateLicense(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.update_license_request.UpdateLicenseRequest;
        const Resp = generated.update_license_response.UpdateLicenseResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const req = parseGeneratedRequest(Req, arena.allocator(), request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0, .error_message = "invalid request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const license = req.license orelse "";
        const license_copy = self.allocator.dupe(u8, license) catch return null;
        if (self.auto_mq_license) |old| self.allocator.free(old);
        self.auto_mq_license = license_copy;
        self.persistAutoMqMetadata();

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .error_message = "" };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleDescribeLicense(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.describe_license_request.DescribeLicenseRequest;
        const Resp = generated.describe_license_response.DescribeLicenseResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        _ = parseGeneratedRequest(Req, arena.allocator(), request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0, .error_message = "invalid request", .license = "" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .error_message = "", .license = self.auto_mq_license orelse "" };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleExportClusterManifest(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.export_cluster_manifest_request.ExportClusterManifestRequest;
        const Resp = generated.export_cluster_manifest_response.ExportClusterManifestResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        _ = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0, .manifest = "" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const manifest = std.fmt.allocPrint(arena_alloc, "{{\"cluster_id\":\"zmq-cluster\",\"node_id\":{d},\"topics\":{d},\"streams\":{d},\"nodes\":{d}}}", .{
            self.node_id,
            self.topics.count(),
            self.object_manager.streamCount(),
            self.auto_mq_nodes.count() + @intFromBool(self.auto_mq_nodes.get(self.node_id) == null),
        }) catch return null;

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .manifest = manifest };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleGetNextNodeId(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.get_next_node_id_request.GetNextNodeIdRequest;
        const Resp = generated.get_next_node_id_response.GetNextNodeIdResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        _ = parseGeneratedRequest(Req, arena.allocator(), request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0, .node_id = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        if (self.auto_mq_next_node_id == std.math.maxInt(i32)) {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0, .node_id = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const node_id = self.auto_mq_next_node_id;
        self.auto_mq_next_node_id += 1;
        self.persistAutoMqMetadata();
        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .node_id = node_id };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleDescribeStreams(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.describe_streams_request.DescribeStreamsRequest;
        const Resp = generated.describe_streams_response.DescribeStreamsResponse;
        const StreamMetadata = Resp.StreamMetadata;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const req = parseGeneratedRequest(Req, arena_alloc, request_bytes, body_start, api_version) catch {
            const resp = Resp{ .error_code = errorCode(.invalid_request), .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        var stream_ids = std.array_list.Managed(u64).init(arena_alloc);
        if (req.stream_id >= 0) {
            stream_ids.append(@intCast(req.stream_id)) catch return null;
        }
        for (req.topic_partitions) |topic_req| {
            const topic_name = topic_req.topic_name orelse continue;
            for (topic_req.partitions) |partition_req| {
                if (partition_req.partition_index < 0) continue;
                stream_ids.append(PartitionStore.hashPartitionKey(topic_name, partition_req.partition_index)) catch return null;
            }
        }
        if (stream_ids.items.len == 0) {
            var it = self.object_manager.streams.iterator();
            while (it.next()) |entry| {
                const stream = entry.value_ptr;
                if (req.node_id >= 0 and stream.node_id != req.node_id) continue;
                stream_ids.append(stream.stream_id) catch return null;
            }
        }

        var metadata = std.array_list.Managed(StreamMetadata).init(arena_alloc);
        for (stream_ids.items) |stream_id| {
            const stream = self.object_manager.getStream(stream_id) orelse continue;
            const mapping = self.findTopicPartitionForStream(stream_id);
            metadata.append(.{
                .stream_id = u64ToI64(stream.stream_id),
                .node_id = stream.node_id,
                .state = if (stream.state == .opened) "OPENED" else "CLOSED",
                .topic_id = if (mapping) |m| m.topic_id else .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
                .topic_name = if (mapping) |m| m.topic_name else null,
                .partition_index = if (mapping) |m| m.partition_index else -1,
                .epoch = u64ToI64(stream.epoch),
                .start_offset = u64ToI64(stream.start_offset),
                .end_offset = u64ToI64(stream.end_offset),
            }) catch return null;
        }

        const resp = Resp{ .error_code = 0, .throttle_time_ms = 0, .stream_metadata_list = metadata.items };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn handleAutomqUpdateGroup(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.automq_update_group_request.AutomqUpdateGroupRequest;
        const Resp = generated.automq_update_group_response.AutomqUpdateGroupResponse;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const req = parseGeneratedRequest(Req, arena.allocator(), request_bytes, body_start, api_version) catch {
            const resp = Resp{ .group_id = null, .error_code = errorCode(.invalid_request), .error_message = "invalid request", .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const group_id = req.group_id orelse "";
        const link_id = req.link_id orelse "";
        if (group_id.len == 0) {
            const resp = Resp{ .group_id = req.group_id, .error_code = errorCode(.invalid_request), .error_message = "group_id is required", .throttle_time_ms = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var mutated = false;
        if (req.promoted) {
            const link_copy = self.allocator.dupe(u8, link_id) catch return null;
            if (self.auto_mq_group_promotions.getPtr(group_id)) |existing| {
                existing.deinit(self.allocator);
                existing.* = .{ .link_id = link_copy, .promoted = true };
            } else {
                const group_copy = self.allocator.dupe(u8, group_id) catch {
                    self.allocator.free(link_copy);
                    return null;
                };
                self.auto_mq_group_promotions.put(group_copy, .{ .link_id = link_copy, .promoted = true }) catch {
                    self.allocator.free(group_copy);
                    self.allocator.free(link_copy);
                    return null;
                };
            }
            mutated = true;
        } else if (self.auto_mq_group_promotions.fetchRemove(group_id)) |removed| {
            self.allocator.free(removed.key);
            var promotion = removed.value;
            promotion.deinit(self.allocator);
            mutated = true;
        }
        if (mutated) self.persistAutoMqMetadata();

        const resp = Resp{ .group_id = req.group_id, .error_code = 0, .error_message = null, .throttle_time_ms = 0 };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // IncrementalAlterConfigs (key 44)
    // Parse config entries and apply valid ones. Handles the same configs
    // as AlterConfigs, with incremental semantics (SET, DELETE, APPEND, SUBTRACT).
    // ---------------------------------------------------------------
    fn handleIncrementalAlterConfigs(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const num_resources = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var results: [32]struct { resource_type: i8, resource_name: []const u8, error_code: i16 } = undefined;
        var count: usize = 0;

        for (0..num_resources) |_| {
            if (count >= 32) break;
            const resource_type = ser.readI8(request_bytes, &pos);
            const resource_name = (ser.readString(request_bytes, &pos) catch break) orelse "";

            // Parse config entries with incremental operations
            const num_configs = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
            for (0..num_configs) |_| {
                const config_name = (ser.readString(request_bytes, &pos) catch break) orelse "";
                _ = ser.readI8(request_bytes, &pos); // config_operation (0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT)
                const config_value = (ser.readString(request_bytes, &pos) catch break) orelse "";

                // Apply config changes to broker/topic
                if (resource_type == 2) {
                    // TOPIC resource — apply topic-level config
                    if (self.topics.getPtr(resource_name)) |info| {
                        if (std.mem.eql(u8, config_name, "retention.ms")) {
                            info.config.retention_ms = std.fmt.parseInt(i64, config_value, 10) catch info.config.retention_ms;
                        } else if (std.mem.eql(u8, config_name, "retention.bytes")) {
                            info.config.retention_bytes = std.fmt.parseInt(i64, config_value, 10) catch info.config.retention_bytes;
                        } else if (std.mem.eql(u8, config_name, "max.message.bytes")) {
                            info.config.max_message_bytes = std.fmt.parseInt(i32, config_value, 10) catch info.config.max_message_bytes;
                        } else if (std.mem.eql(u8, config_name, "min.insync.replicas")) {
                            info.config.min_insync_replicas = std.fmt.parseInt(i32, config_value, 10) catch info.config.min_insync_replicas;
                        }
                    }
                }
            }

            results[count] = .{
                .resource_type = resource_type,
                .resource_name = resource_name,
                .error_code = 0, // SUCCESS
            };
            count += 1;
        }

        var buf = self.allocator.alloc(u8, 1024) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, count);
        for (0..count) |i| {
            ser.writeI16(buf, &wpos, results[i].error_code);
            ser.writeString(buf, &wpos, null); // error_message
            ser.writeI8(buf, &wpos, results[i].resource_type);
            ser.writeString(buf, &wpos, results[i].resource_name);
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // OffsetDelete (key 47)
    // Delete committed offsets for specified topic-partitions from
    // the group coordinator.
    // ---------------------------------------------------------------
    fn handleOffsetDelete(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;

        // Parse: group_id
        const group_id = (ser.readString(request_bytes, &pos) catch return null) orelse "";

        // Parse topics array
        const num_topics = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code = NONE
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms

        ser.writeArrayLen(buf, &wpos, num_topics);

        for (0..num_topics) |_| {
            const topic_name = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const num_partitions = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

            ser.writeString(buf, &wpos, topic_name);
            ser.writeArrayLen(buf, &wpos, num_partitions);

            for (0..num_partitions) |_| {
                const partition_id = ser.readI32(request_bytes, &pos);

                // Delete the committed offset from group coordinator
                // Build the offset key and remove it
                const key = std.fmt.allocPrint(self.allocator, "{s}:{s}:{d}", .{ group_id, topic_name, partition_id }) catch continue;
                if (self.groups.committed_offsets.fetchRemove(key)) |old| {
                    self.allocator.free(old.key);
                } else {
                    self.allocator.free(key);
                }

                ser.writeI32(buf, &wpos, partition_id);
                ser.writeI16(buf, &wpos, 0); // error_code = NONE
            }
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // Unsupported
    // ---------------------------------------------------------------
    fn handleUnsupported(self: *Broker, req_header: *const RequestHeader, api_key: i16, resp_header_version: i16) ?[]u8 {
        log.warn("Unsupported API key: {d}", .{api_key});
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const header_size = resp_header.calcSize(resp_header_version);
        const buf = self.allocator.alloc(u8, header_size + 2) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 35); // UNSUPPORTED_VERSION
        return buf[0..wpos];
    }

    /// Return a NOT_CONTROLLER error response (error code 41).
    /// Sent when a KRaft API is received on a broker-only node.
    fn handleNotController(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const header_size = resp_header.calcSize(resp_header_version);
        const buf = self.allocator.alloc(u8, header_size + 2) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 41); // NOT_CONTROLLER
        return buf[0..wpos];
    }

    /// Return NOT_LEADER_OR_FOLLOWER (error code 6) during graceful shutdown.
    /// Clients receiving this error will refresh metadata and reconnect to another broker,
    /// achieving seamless traffic migration during rolling restarts.
    fn handleShutdownReject(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const header_size = resp_header.calcSize(resp_header_version);
        const buf = self.allocator.alloc(u8, header_size + 2) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 6); // NOT_LEADER_OR_FOLLOWER
        return buf[0..wpos];
    }

    // ---------------------------------------------------------------
    // SaslHandshake (key 17)
    // ---------------------------------------------------------------
    fn handleSaslHandshake(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        const mechanism = (ser.readString(request_bytes, &pos) catch return null) orelse "";

        var buf = self.allocator.alloc(u8, 256) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);

        // Check if the requested mechanism is enabled in sasl_enabled_mechanisms
        const is_enabled = isMechanismEnabled(self.sasl_enabled_mechanisms, mechanism);
        if (is_enabled) {
            ser.writeI16(buf, &wpos, 0); // error_code = NONE
        } else {
            ser.writeI16(buf, &wpos, 33); // error_code = UNSUPPORTED_SASL_MECHANISM
        }

        // Store the negotiated mechanism for this client so handleSaslAuthenticate
        // knows which authenticator to use
        if (is_enabled) {
            if (req_header.client_id) |client_id| {
                const key = self.allocator.dupe(u8, client_id) catch {
                    log.warn("SASL handshake: failed to store mechanism for client", .{});
                    return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                };
                const val = self.allocator.dupe(u8, mechanism) catch {
                    self.allocator.free(key);
                    log.warn("SASL handshake: failed to store mechanism for client", .{});
                    return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                };
                // Remove old entry if client re-handshakes
                if (self.sasl_mechanisms.fetchRemove(key)) |old| {
                    self.allocator.free(old.key);
                    self.allocator.free(old.value);
                }
                self.sasl_mechanisms.put(key, val) catch {
                    self.allocator.free(key);
                    self.allocator.free(val);
                    log.warn("SASL handshake: failed to store mechanism for client", .{});
                };
            }
        }

        // Advertise all enabled mechanisms
        const mech_count = countMechanisms(self.sasl_enabled_mechanisms);
        ser.writeArrayLen(buf, &wpos, @intCast(mech_count));
        writeMechanismNames(self.sasl_enabled_mechanisms, buf, &wpos);
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    /// Check whether a given mechanism name appears in the comma-separated enabled list.
    fn isMechanismEnabled(enabled: []const u8, mechanism: []const u8) bool {
        var iter = std.mem.splitSequence(u8, enabled, ",");
        while (iter.next()) |m| {
            if (std.mem.eql(u8, m, mechanism)) return true;
        }
        return false;
    }

    /// Count the number of mechanisms in a comma-separated list.
    fn countMechanisms(enabled: []const u8) usize {
        if (enabled.len == 0) return 0;
        var count: usize = 0;
        var iter = std.mem.splitSequence(u8, enabled, ",");
        while (iter.next()) |_| count += 1;
        return count;
    }

    /// Write each mechanism name from the comma-separated list into the response buffer.
    fn writeMechanismNames(enabled: []const u8, buf: []u8, wpos: *usize) void {
        var iter = std.mem.splitSequence(u8, enabled, ",");
        while (iter.next()) |m| {
            ser.writeString(buf, wpos, m);
        }
    }

    // ---------------------------------------------------------------
    // SaslAuthenticate (key 36)
    // ---------------------------------------------------------------
    fn handleSaslAuthenticate(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        // auth_bytes contains the SASL credentials (format depends on mechanism)
        const auth_bytes = ser.readBytes(request_bytes, &pos) catch null;

        var buf = self.allocator.alloc(u8, 512) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);

        // If SASL is not enabled, accept all connections (backward compatible)
        if (!self.sasl_enabled) {
            ser.writeI16(buf, &wpos, 0); // error_code = NONE
            ser.writeString(buf, &wpos, ""); // error_message
            ser.writeBytesBuf(buf, &wpos, &.{}); // auth_bytes (empty response)
            ser.writeI64(buf, &wpos, 0); // session_lifetime_ms
            if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
            return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
        }

        // Determine which mechanism this client negotiated during SaslHandshake
        const client_id = req_header.client_id orelse "anonymous";
        const mechanism = self.sasl_mechanisms.get(client_id) orelse "PLAIN";

        if (std.mem.eql(u8, mechanism, "OAUTHBEARER")) {
            // OAUTHBEARER: parse JWT from SASL token (KIP-255)
            if (auth_bytes) |token| {
                const result = self.oauth_authenticator.authenticate(self.allocator, token);
                if (result.success) {
                    if (result.principal) |principal| {
                        // Format as "User:<sub-claim>" to match Kafka principal format
                        const full_principal = std.fmt.allocPrint(self.allocator, "User:{s}", .{principal}) catch {
                            ser.writeI16(buf, &wpos, 58); // SASL_AUTHENTICATION_FAILED
                            ser.writeString(buf, &wpos, "Internal error");
                            ser.writeBytesBuf(buf, &wpos, &.{});
                            ser.writeI64(buf, &wpos, 0);
                            if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                            return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                        };
                        self.storeAuthenticatedPrincipal(client_id, full_principal);
                    }
                    ser.writeI16(buf, &wpos, 0); // error_code = NONE
                    ser.writeString(buf, &wpos, ""); // error_message
                    ser.writeBytesBuf(buf, &wpos, &.{}); // auth_bytes
                    ser.writeI64(buf, &wpos, 0); // session_lifetime_ms
                    if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                    return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                }
            }
            // OAUTHBEARER authentication failed
            ser.writeI16(buf, &wpos, 58); // error_code = SASL_AUTHENTICATION_FAILED
            ser.writeString(buf, &wpos, "OAUTHBEARER authentication failed");
            ser.writeBytesBuf(buf, &wpos, &.{});
            ser.writeI64(buf, &wpos, 0);
            if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
            return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
        } else if (std.mem.eql(u8, mechanism, "SCRAM-SHA-256")) {
            // SCRAM-SHA-256: RFC 5802 / RFC 7677
            // Multi-round exchange: client-first → server-first → client-final → server-final.
            // Each SaslAuthenticate call progresses the per-connection ScramStateMachine.
            const token = auth_bytes orelse {
                ser.writeI16(buf, &wpos, 58); // SASL_AUTHENTICATION_FAILED
                ser.writeString(buf, &wpos, "Missing SCRAM auth bytes");
                ser.writeBytesBuf(buf, &wpos, &.{});
                ser.writeI64(buf, &wpos, 0);
                if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
            };

            // Look up or create SCRAM session for this client
            const scram_entry = self.scram_sessions.getPtr(client_id);
            if (scram_entry) |sm| {
                // Existing session — this should be the client-final-message (round 2)
                if (sm.state == .server_first_sent) {
                    if (sm.handleClientFinal(&self.scram_authenticator, token)) |server_final| {
                        defer self.allocator.free(server_final);
                        // Authentication succeeded — store principal and return server-final
                        if (sm.username) |username| {
                            const full_principal = std.fmt.allocPrint(self.allocator, "User:{s}", .{username}) catch {
                                ser.writeI16(buf, &wpos, 58);
                                ser.writeString(buf, &wpos, "Internal error");
                                ser.writeBytesBuf(buf, &wpos, &.{});
                                ser.writeI64(buf, &wpos, 0);
                                if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                                return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                            };
                            self.storeAuthenticatedPrincipal(client_id, full_principal);
                        }
                        ser.writeI16(buf, &wpos, 0); // error_code = NONE
                        ser.writeString(buf, &wpos, ""); // error_message
                        ser.writeBytesBuf(buf, &wpos, server_final); // server-final-message
                        ser.writeI64(buf, &wpos, 0); // session_lifetime_ms
                        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                        // Clean up the completed SCRAM session
                        if (self.scram_sessions.fetchRemove(client_id)) |old| {
                            self.allocator.free(old.key);
                            var old_sm = old.value;
                            old_sm.deinit();
                        }
                        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                    } else {
                        // Client proof verification failed
                        ser.writeI16(buf, &wpos, 58); // SASL_AUTHENTICATION_FAILED
                        ser.writeString(buf, &wpos, "SCRAM-SHA-256 authentication failed");
                        ser.writeBytesBuf(buf, &wpos, &.{});
                        ser.writeI64(buf, &wpos, 0);
                        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                        // Clean up the failed session
                        if (self.scram_sessions.fetchRemove(client_id)) |old| {
                            self.allocator.free(old.key);
                            var old_sm = old.value;
                            old_sm.deinit();
                        }
                        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                    }
                } else {
                    // Unexpected state — session exists but not in server_first_sent
                    ser.writeI16(buf, &wpos, 58);
                    ser.writeString(buf, &wpos, "SCRAM-SHA-256 unexpected state");
                    ser.writeBytesBuf(buf, &wpos, &.{});
                    ser.writeI64(buf, &wpos, 0);
                    if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                    if (self.scram_sessions.fetchRemove(client_id)) |old| {
                        self.allocator.free(old.key);
                        var old_sm = old.value;
                        old_sm.deinit();
                    }
                    return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                }
            } else {
                // No existing session — this is the client-first-message (round 1)
                var sm = ScramStateMachine.init(self.allocator);
                if (sm.handleClientFirst(&self.scram_authenticator, token)) |server_first| {
                    defer self.allocator.free(server_first);
                    // Store the state machine for this client's next round
                    const key = self.allocator.dupe(u8, client_id) catch {
                        sm.deinit();
                        ser.writeI16(buf, &wpos, 58);
                        ser.writeString(buf, &wpos, "Internal error");
                        ser.writeBytesBuf(buf, &wpos, &.{});
                        ser.writeI64(buf, &wpos, 0);
                        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                    };
                    self.scram_sessions.put(key, sm) catch {
                        self.allocator.free(key);
                        sm.deinit();
                        ser.writeI16(buf, &wpos, 58);
                        ser.writeString(buf, &wpos, "Internal error");
                        ser.writeBytesBuf(buf, &wpos, &.{});
                        ser.writeI64(buf, &wpos, 0);
                        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                    };
                    // Return server-first-message to client (intermediate challenge)
                    // Kafka sends error_code=0 for SCRAM challenge responses; the client
                    // knows the exchange is not yet complete from the SCRAM state machine.
                    ser.writeI16(buf, &wpos, 0); // error_code = NONE (continue exchange)
                    ser.writeString(buf, &wpos, ""); // error_message
                    ser.writeBytesBuf(buf, &wpos, server_first); // server-first-message
                    ser.writeI64(buf, &wpos, 0);
                    if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                    return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                } else {
                    // Failed to parse client-first (unknown user, bad format, etc.)
                    sm.deinit();
                    ser.writeI16(buf, &wpos, 58); // SASL_AUTHENTICATION_FAILED
                    ser.writeString(buf, &wpos, "SCRAM-SHA-256 authentication failed");
                    ser.writeBytesBuf(buf, &wpos, &.{});
                    ser.writeI64(buf, &wpos, 0);
                    if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                    return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                }
            }
        } else {
            // PLAIN (default) — existing SASL/PLAIN authentication path
            if (auth_bytes) |token| {
                const result = self.sasl_authenticator.authenticate(token);
                if (result.success) {
                    if (result.principal) |principal| {
                        // Format as "User:<username>" to match Kafka principal format
                        const full_principal = std.fmt.allocPrint(self.allocator, "User:{s}", .{principal}) catch {
                            ser.writeI16(buf, &wpos, 58); // SASL_AUTHENTICATION_FAILED
                            ser.writeString(buf, &wpos, "Internal error");
                            ser.writeBytesBuf(buf, &wpos, &.{});
                            ser.writeI64(buf, &wpos, 0);
                            if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                            return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                        };
                        self.storeAuthenticatedPrincipal(client_id, full_principal);
                    }
                    ser.writeI16(buf, &wpos, 0); // error_code = NONE
                    ser.writeString(buf, &wpos, ""); // error_message
                    ser.writeBytesBuf(buf, &wpos, &.{}); // auth_bytes
                    ser.writeI64(buf, &wpos, 0); // session_lifetime_ms
                    if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
                    return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
                }
            }

            // PLAIN authentication failed
            ser.writeI16(buf, &wpos, 58); // error_code = SASL_AUTHENTICATION_FAILED
            ser.writeString(buf, &wpos, "Authentication failed");
            ser.writeBytesBuf(buf, &wpos, &.{}); // auth_bytes
            ser.writeI64(buf, &wpos, 0); // session_lifetime_ms
            if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
            return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
        }
    }

    /// Store an authenticated principal in the session map.
    /// Takes ownership of `full_principal` — caller must not free it on success.
    fn storeAuthenticatedPrincipal(self: *Broker, client_id: []const u8, full_principal: []u8) void {
        const key = self.allocator.dupe(u8, client_id) catch {
            self.allocator.free(full_principal);
            log.warn("Failed to store authenticated principal: allocation failed", .{});
            return;
        };
        // Remove old session entry if client re-authenticates
        if (self.authenticated_sessions.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
        self.authenticated_sessions.put(key, full_principal) catch {
            self.allocator.free(key);
            self.allocator.free(full_principal);
            log.warn("Failed to store authenticated principal: map insertion failed", .{});
        };
    }

    // ---------------------------------------------------------------
    // OffsetForLeaderEpoch (key 23)
    // ---------------------------------------------------------------
    fn handleOffsetForLeaderEpoch(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        // v2+: replica_id
        _ = ser.readI32(request_bytes, &pos); // replica_id

        const num_topics = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, num_topics);

        for (0..num_topics) |_| {
            const topic = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const num_parts = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

            ser.writeString(buf, &wpos, topic);
            ser.writeArrayLen(buf, &wpos, num_parts);

            for (0..num_parts) |_| {
                const partition = ser.readI32(request_bytes, &pos);
                _ = ser.readI32(request_bytes, &pos); // current_leader_epoch
                const leader_epoch = ser.readI32(request_bytes, &pos);
                _ = leader_epoch;

                // Get the end offset for this leader epoch
                const pkey = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ topic, partition }) catch continue;
                defer self.allocator.free(pkey);
                const end_offset: i64 = if (self.store.partitions.get(pkey)) |state|
                    @intCast(state.high_watermark)
                else
                    -1;

                ser.writeI16(buf, &wpos, 0); // error_code
                ser.writeI32(buf, &wpos, partition);
                ser.writeI32(buf, &wpos, if (self.raft_state) |rs| rs.current_epoch else self.cached_leader_epoch); // leader_epoch
                ser.writeI64(buf, &wpos, end_offset); // end_offset
            }
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // WriteTxnMarkers (key 27) — Write actual control batches
    // ---------------------------------------------------------------
    fn handleWriteTxnMarkers(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const num_markers = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var buf = self.allocator.alloc(u8, 2048) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeArrayLen(buf, &wpos, num_markers);

        for (0..num_markers) |_| {
            const producer_id = ser.readI64(request_bytes, &pos);
            const producer_epoch = ser.readI16(request_bytes, &pos);
            const committed = ser.readBool(request_bytes, &pos) catch false;

            // Write markers for each topic-partition
            const num_topics = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

            // Actually write control record batches to each partition
            // before transitioning state, not just updating the coordinator.
            const control_type: TxnCoordinator.ControlRecordType = if (committed) .commit else .abort;
            var partition_state_dirty = false;
            if (self.txn_coordinator.getPartitions(producer_id)) |partitions| {
                for (partitions) |tp| {
                    const pkey = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ tp.topic, tp.partition }) catch continue;
                    defer self.allocator.free(pkey);
                    const base_off: i64 = if (self.store.partitions.get(pkey)) |state|
                        @intCast(state.next_offset)
                    else
                        0;

                    const control_batch = self.txn_coordinator.buildControlBatch(
                        producer_id,
                        producer_epoch,
                        control_type,
                        base_off,
                    ) catch continue;
                    defer self.allocator.free(control_batch);

                    // Write the control batch to the partition store
                    _ = self.store.produce(tp.topic, tp.partition, control_batch) catch |err| {
                        log.warn("Principle 7: Failed to write control batch for {s}-{d}: {}", .{ tp.topic, tp.partition, err });
                        continue;
                    };
                    partition_state_dirty = true;
                }

                // Clear LSO on each partition after writing markers
                // NOTE: AutoMQ/Kafka advances LSO after control batch markers are written.
                // Must happen before writeTxnMarkers() which clears the partition list.
                for (partitions) |tp| {
                    const lso_key = std.fmt.allocPrint(self.allocator, "{s}-{d}", .{ tp.topic, tp.partition }) catch continue;
                    defer self.allocator.free(lso_key);
                    if (self.store.partitions.getPtr(lso_key)) |pstate| {
                        pstate.first_unstable_txn_offset = null;
                        pstate.last_stable_offset = pstate.high_watermark;
                        partition_state_dirty = true;
                    }
                }
            }
            if (partition_state_dirty) {
                self.persistPartitionStates();
                self.persistObjectManagerSnapshot();
            }

            // Now complete the state transition
            _ = self.txn_coordinator.writeTxnMarkers(producer_id);

            ser.writeI64(buf, &wpos, producer_id);
            ser.writeArrayLen(buf, &wpos, num_topics);

            for (0..num_topics) |_| {
                const topic = (ser.readString(request_bytes, &pos) catch break) orelse "";
                const num_parts = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

                ser.writeString(buf, &wpos, topic);
                ser.writeArrayLen(buf, &wpos, num_parts);

                for (0..num_parts) |_| {
                    const partition = ser.readI32(request_bytes, &pos);
                    ser.writeI32(buf, &wpos, partition);
                    ser.writeI16(buf, &wpos, 0); // error_code = NONE
                }
            }
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // TxnOffsetCommit (key 28)
    // ---------------------------------------------------------------
    fn handleTxnOffsetCommit(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const transactional_id = (ser.readString(request_bytes, &pos) catch return null) orelse "";
        _ = transactional_id;
        const group_id = (ser.readString(request_bytes, &pos) catch return null) orelse "";
        const producer_id = ser.readI64(request_bytes, &pos);
        _ = producer_id;
        const producer_epoch = ser.readI16(request_bytes, &pos);
        _ = producer_epoch;

        // Parse topics and commit offsets
        const num_topics = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, num_topics);

        for (0..num_topics) |_| {
            const topic = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const num_parts = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;

            ser.writeString(buf, &wpos, topic);
            ser.writeArrayLen(buf, &wpos, num_parts);

            for (0..num_parts) |_| {
                const partition = ser.readI32(request_bytes, &pos);
                const offset = ser.readI64(request_bytes, &pos);

                // Commit the offset through the group coordinator
                self.groups.commitOffset(group_id, topic, partition, offset) catch {};

                ser.writeI32(buf, &wpos, partition);
                ser.writeI16(buf, &wpos, 0); // error_code = NONE
            }
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DescribeAcls (key 29)
    // ---------------------------------------------------------------
    fn handleDescribeAcls(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var buf = self.allocator.alloc(u8, 4096) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, 0); // error_code = NONE
        ser.writeString(buf, &wpos, null); // error_message

        // Return all ACLs from the authorizer
        const acl_count = self.authorizer.aclCount();
        ser.writeArrayLen(buf, &wpos, acl_count);
        for (self.authorizer.acls.items) |acl| {
            ser.writeI8(buf, &wpos, @intFromEnum(acl.resource_type));
            ser.writeString(buf, &wpos, acl.resource_name);
            ser.writeI8(buf, &wpos, @intFromEnum(acl.pattern_type));
            // ACL entries sub-array (1 entry per ACL)
            ser.writeArrayLen(buf, &wpos, 1);
            ser.writeString(buf, &wpos, acl.principal);
            ser.writeString(buf, &wpos, acl.host);
            ser.writeI8(buf, &wpos, @intFromEnum(acl.operation));
            ser.writeI8(buf, &wpos, @intFromEnum(acl.permission));
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // CreateAcls (key 30)
    // ---------------------------------------------------------------
    fn handleCreateAcls(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;
        const num_creations = (ser.readArrayLen(request_bytes, &pos) catch 0) orelse 0;
        var results: [64]i16 = undefined;
        var count: usize = 0;

        for (0..num_creations) |_| {
            if (count >= 64) break;
            const resource_type = ser.readI8(request_bytes, &pos);
            const resource_name = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const pattern_type = ser.readI8(request_bytes, &pos);
            const principal = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const host = (ser.readString(request_bytes, &pos) catch break) orelse "";
            const operation = ser.readI8(request_bytes, &pos);
            const permission = ser.readI8(request_bytes, &pos);

            self.authorizer.addAcl(
                principal,
                @enumFromInt(resource_type),
                resource_name,
                @enumFromInt(pattern_type),
                @enumFromInt(operation),
                @enumFromInt(permission),
                host,
            ) catch {
                results[count] = 3; // SECURITY_DISABLED
                count += 1;
                continue;
            };
            results[count] = 0; // success
            count += 1;
        }

        var buf = self.allocator.alloc(u8, 1024) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, count);
        for (0..count) |i| {
            ser.writeI16(buf, &wpos, results[i]); // error_code
            ser.writeString(buf, &wpos, null); // error_message
        }
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        // Persist ACLs after creation
        self.persistAcls();

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DeleteAcls (key 31)
    // ---------------------------------------------------------------
    fn handleDeleteAcls(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        var pos = body_start;

        // Parse filter array
        const num_filters_opt = ser.readArrayLen(request_bytes, &pos) catch null;
        const num_filters: usize = if (num_filters_opt) |n| @min(n, 64) else 0;

        var buf = self.allocator.alloc(u8, 512) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeArrayLen(buf, &wpos, @intCast(num_filters)); // filter results

        var total_removed: usize = 0;
        for (0..num_filters) |_| {
            // Parse filter: resource_type (i8), resource_name (nullable string),
            // pattern_type (i8), principal (nullable string), host (nullable string),
            // operation (i8), permission (i8)
            const rt_raw = ser.readI8(request_bytes, &pos);
            const rn = (ser.readString(request_bytes, &pos) catch null) orelse "*";
            const pt_raw = ser.readI8(request_bytes, &pos);
            const principal = (ser.readString(request_bytes, &pos) catch null) orelse "*";
            const host = (ser.readString(request_bytes, &pos) catch null) orelse "*";
            const op_raw = ser.readI8(request_bytes, &pos);
            const perm_raw = ser.readI8(request_bytes, &pos);

            const resource_type: Authorizer.ResourceType = @enumFromInt(rt_raw);
            const pattern_type: Authorizer.PatternType = @enumFromInt(pt_raw);
            const operation: Authorizer.Operation = @enumFromInt(op_raw);
            const permission: Authorizer.Permission = @enumFromInt(perm_raw);

            const removed = self.authorizer.removeMatchingAcls(resource_type, rn, pattern_type, principal, host, operation, permission);
            total_removed += removed;

            // Write per-filter result: error_code (i16), error_message (nullable string), matching_acls array
            ser.writeI16(buf, &wpos, 0); // error_code = NONE
            ser.writeString(buf, &wpos, ""); // error_message
            ser.writeArrayLen(buf, &wpos, @intCast(removed)); // matching_acls count
            // NOTE: Kafka protocol expects per-match details; we return count only for simplicity
        }

        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        // Persist ACLs after deletion
        if (total_removed > 0) {
            self.persistAcls();
        }

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DescribeLogDirs (key 35)
    // ---------------------------------------------------------------
    fn handleDescribeLogDirs(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.describe_log_dirs_request.DescribeLogDirsRequest;
        const Resp = generated.describe_log_dirs_response.DescribeLogDirsResponse;
        const Result = Resp.DescribeLogDirsResult;

        var pos = body_start;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode DescribeLogDirs request: {}", .{err});
            return null;
        };
        defer self.freeDescribeLogDirsRequest(&req);

        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const log_dir = if (self.store.data_dir) |d| d else "/data/automq";
        const topics = self.collectDescribeLogDirsTopics(&req) catch return null;
        defer {
            self.freeDescribeLogDirsTopics(topics);
            if (topics.len > 0) self.allocator.free(topics);
        }

        const results = [_]Result{.{
            .error_code = @intFromEnum(ErrorCode.none),
            .log_dir = log_dir,
            .topics = topics,
            .total_bytes = -1,
            .usable_bytes = -1,
        }};
        const resp_body = Resp{
            .throttle_time_ms = 0,
            .error_code = @intFromEnum(ErrorCode.none),
            .results = &results,
        };

        const needed = rh.calcSize(resp_header_version) + resp_body.calcSize(api_version);
        var buf = self.allocator.alloc(u8, needed) catch return null;
        var wpos: usize = 0;
        rh.serialize(buf, &wpos, resp_header_version);
        resp_body.serialize(buf, &wpos, api_version);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    fn freeDescribeLogDirsRequest(self: *Broker, req: *generated.describe_log_dirs_request.DescribeLogDirsRequest) void {
        for (req.topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (req.topics.len > 0) self.allocator.free(req.topics);
    }

    fn collectDescribeLogDirsTopics(self: *Broker, req: *const generated.describe_log_dirs_request.DescribeLogDirsRequest) ![]generated.describe_log_dirs_response.DescribeLogDirsResponse.DescribeLogDirsResult.DescribeLogDirsTopic {
        const Topic = generated.describe_log_dirs_response.DescribeLogDirsResponse.DescribeLogDirsResult.DescribeLogDirsTopic;

        var topics = std.array_list.Managed(Topic).init(self.allocator);
        errdefer {
            self.freeDescribeLogDirsTopics(topics.items);
            topics.deinit();
        }

        if (req.topics.len == 0) {
            var topic_iter = self.topics.iterator();
            while (topic_iter.next()) |entry| {
                const info = entry.value_ptr;
                const partitions = try self.collectDescribeLogDirsPartitions(info.name, info.*, &.{});
                topics.append(.{
                    .name = info.name,
                    .partitions = partitions,
                }) catch |err| {
                    if (partitions.len > 0) self.allocator.free(partitions);
                    return err;
                };
            }
        } else {
            for (req.topics) |topic_req| {
                const topic_name = topic_req.topic orelse "";
                const info = self.topics.get(topic_name);
                const partitions = try self.collectDescribeLogDirsPartitions(topic_name, info, topic_req.partitions);
                topics.append(.{
                    .name = topic_req.topic,
                    .partitions = partitions,
                }) catch |err| {
                    if (partitions.len > 0) self.allocator.free(partitions);
                    return err;
                };
            }
        }

        if (topics.items.len == 0) return &.{};
        return topics.toOwnedSlice();
    }

    fn collectDescribeLogDirsPartitions(self: *Broker, topic_name: []const u8, topic_info: ?TopicInfo, requested_partitions: []const i32) ![]generated.describe_log_dirs_response.DescribeLogDirsResponse.DescribeLogDirsResult.DescribeLogDirsTopic.DescribeLogDirsPartition {
        const Partition = generated.describe_log_dirs_response.DescribeLogDirsResponse.DescribeLogDirsResult.DescribeLogDirsTopic.DescribeLogDirsPartition;
        const info = topic_info orelse return &.{};

        var partitions = std.array_list.Managed(Partition).init(self.allocator);
        errdefer partitions.deinit();

        if (requested_partitions.len == 0) {
            const num_parts: usize = @intCast(@max(info.num_partitions, 0));
            for (0..num_parts) |partition_index| {
                try partitions.append(self.describeLogDirPartition(topic_name, @intCast(partition_index)));
            }
        } else {
            for (requested_partitions) |partition_index| {
                if (partition_index < 0 or partition_index >= info.num_partitions) continue;
                try partitions.append(self.describeLogDirPartition(topic_name, partition_index));
            }
        }

        if (partitions.items.len == 0) return &.{};
        return partitions.toOwnedSlice();
    }

    fn describeLogDirPartition(self: *Broker, topic_name: []const u8, partition_index: i32) generated.describe_log_dirs_response.DescribeLogDirsResponse.DescribeLogDirsResult.DescribeLogDirsTopic.DescribeLogDirsPartition {
        return .{
            .partition_index = partition_index,
            .partition_size = self.estimateLogDirPartitionSize(topic_name, partition_index),
            .offset_lag = 0,
            .is_future_key = false,
        };
    }

    fn estimateLogDirPartitionSize(self: *Broker, topic_name: []const u8, partition_index: i32) i64 {
        const stream_id = PartitionStore.hashPartitionKey(topic_name, partition_index);
        var cached_bytes: u64 = 0;
        for (self.store.cache.blocks.items) |block| {
            for (block.records.items) |record| {
                if (record.stream_id == stream_id) cached_bytes += record.data.len;
            }
        }
        if (cached_bytes > 0) return clampU64ToI64(cached_bytes);

        var key_buf: [256]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}-{d}", .{ topic_name, partition_index }) catch return 0;
        if (self.store.partitions.get(key)) |state| {
            return clampU64ToI64(state.next_offset);
        }
        return 0;
    }

    fn clampU64ToI64(value: u64) i64 {
        const max_i64: u64 = @intCast(std.math.maxInt(i64));
        return if (value > max_i64) std.math.maxInt(i64) else @intCast(value);
    }

    fn freeDescribeLogDirsTopics(self: *Broker, topics: []const generated.describe_log_dirs_response.DescribeLogDirsResponse.DescribeLogDirsResult.DescribeLogDirsTopic) void {
        for (topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
    }

    // ---------------------------------------------------------------
    // ElectLeaders (key 43)
    // ---------------------------------------------------------------
    fn handleElectLeaders(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.elect_leaders_request.ElectLeadersRequest;
        const Resp = generated.elect_leaders_response.ElectLeadersResponse;
        const Result = Resp.ReplicaElectionResult;
        const PartitionResult = Result.PartitionResult;

        var pos = body_start;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode ElectLeaders request: {}", .{err});
            return null;
        };
        defer self.freeElectLeadersRequest(&req);

        const top_error: i16 = if (req.election_type == 0 or req.election_type == 1)
            @intFromEnum(ErrorCode.none)
        else
            @intFromEnum(ErrorCode.invalid_request);

        // Trigger a leader election via Raft
        if (top_error == 0) {
            if (self.raft_state) |raft| {
                if (raft.role != .leader) {
                    // If we're not the leader, start an election
                    _ = raft.startElection();
                    if (raft.quorumSize() <= 1) {
                        raft.becomeLeader();
                    }
                    log.info("Leader election triggered via ElectLeaders API", .{});
                }
            }
        }

        var results = std.array_list.Managed(Result).init(self.allocator);
        defer {
            for (results.items) |result| self.allocator.free(result.partition_result);
            results.deinit();
        }

        if (top_error == 0) {
            if (req.topic_partitions.len == 0) {
                var tit = self.topics.iterator();
                while (tit.next()) |entry| {
                    const info = entry.value_ptr;
                    const partition_count: usize = @intCast(info.num_partitions);
                    const partition_results = self.allocator.alloc(PartitionResult, partition_count) catch return null;
                    for (0..partition_count) |pi| {
                        partition_results[pi] = .{
                            .partition_id = @intCast(pi),
                            .error_code = @intFromEnum(ErrorCode.none),
                            .error_message = null,
                        };
                    }
                    results.append(.{ .topic = info.name, .partition_result = partition_results }) catch {
                        self.allocator.free(partition_results);
                        return null;
                    };
                }
            } else {
                for (req.topic_partitions) |topic_partitions| {
                    const topic_name = topic_partitions.topic orelse "";
                    const info = self.topics.get(topic_name);
                    const requested_count = topic_partitions.partitions.len;
                    const partition_count: usize = if (requested_count > 0) requested_count else if (info) |topic_info| @intCast(topic_info.num_partitions) else 1;

                    const partition_results = self.allocator.alloc(PartitionResult, partition_count) catch return null;

                    if (info) |topic_info| {
                        for (0..partition_count) |i| {
                            const partition_id: i32 = if (requested_count > 0) topic_partitions.partitions[i] else @intCast(i);
                            const error_code: i16 = if (partition_id >= 0 and partition_id < topic_info.num_partitions)
                                @intFromEnum(ErrorCode.none)
                            else
                                @intFromEnum(ErrorCode.unknown_topic_or_partition);
                            partition_results[i] = .{
                                .partition_id = partition_id,
                                .error_code = error_code,
                                .error_message = null,
                            };
                        }
                    } else {
                        for (0..partition_count) |i| {
                            partition_results[i] = .{
                                .partition_id = if (requested_count > 0) topic_partitions.partitions[i] else -1,
                                .error_code = @intFromEnum(ErrorCode.unknown_topic_or_partition),
                                .error_message = null,
                            };
                        }
                    }

                    results.append(.{ .topic = topic_partitions.topic, .partition_result = partition_results }) catch {
                        self.allocator.free(partition_results);
                        return null;
                    };
                }
            }
        }

        const resp_body = Resp{
            .throttle_time_ms = 0,
            .error_code = top_error,
            .replica_election_results = results.items,
        };
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const needed = rh.calcSize(resp_header_version) + resp_body.calcSize(api_version);
        var buf = self.allocator.alloc(u8, needed) catch return null;
        var wpos: usize = 0;
        rh.serialize(buf, &wpos, resp_header_version);
        resp_body.serialize(buf, &wpos, api_version);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    fn freeElectLeadersRequest(self: *Broker, req: *generated.elect_leaders_request.ElectLeadersRequest) void {
        for (req.topic_partitions) |topic_partitions| {
            if (topic_partitions.partitions.len > 0) self.allocator.free(topic_partitions.partitions);
        }
        if (req.topic_partitions.len > 0) self.allocator.free(req.topic_partitions);
    }

    // ---------------------------------------------------------------
    // DescribeCluster (key 60) — Return actual broker state
    // ---------------------------------------------------------------
    fn handleDescribeCluster(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var buf = self.allocator.alloc(u8, 512) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeString(buf, &wpos, ""); // error_message
        ser.writeString(buf, &wpos, if (self.raft_state) |rs| rs.cluster_id else "zmq-cluster"); // cluster_id — use actual cluster ID
        ser.writeI32(buf, &wpos, self.node_id); // controller_id
        // Brokers array: 1 broker (single-node mode)
        ser.writeArrayLen(buf, &wpos, 1);
        ser.writeI32(buf, &wpos, self.node_id); // broker_id
        ser.writeString(buf, &wpos, self.advertised_host); // host
        ser.writeI32(buf, &wpos, @as(i32, self.port)); // port
        ser.writeString(buf, &wpos, ""); // rack (no rack awareness in single-node)
        ser.writeI32(buf, &wpos, 0); // cluster_authorized_operations
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DescribeProducers (key 61) — Return request-scoped producer state
    // ---------------------------------------------------------------
    fn handleDescribeProducers(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.describe_producers_request.DescribeProducersRequest;
        const Resp = generated.describe_producers_response.DescribeProducersResponse;
        const TopicResponse = Resp.TopicResponse;
        const PartitionResponse = TopicResponse.PartitionResponse;

        var pos = body_start;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode DescribeProducers request: {}", .{err});
            return null;
        };
        defer self.freeDescribeProducersRequest(&req);

        const topic_responses = self.allocator.alloc(TopicResponse, req.topics.len) catch return null;
        var topic_init: usize = 0;
        defer {
            self.freeDescribeProducersTopicResponses(topic_responses[0..topic_init]);
            if (topic_responses.len > 0) self.allocator.free(topic_responses);
        }

        for (req.topics) |topic_req| {
            const topic_name = topic_req.name orelse "";
            const topic_info = self.topics.get(topic_name);
            const partition_responses = self.allocator.alloc(PartitionResponse, topic_req.partition_indexes.len) catch return null;
            var partition_init: usize = 0;
            var transferred = false;
            defer {
                if (!transferred) {
                    self.freeDescribeProducersPartitions(partition_responses[0..partition_init]);
                    if (partition_responses.len > 0) self.allocator.free(partition_responses);
                }
            }

            for (topic_req.partition_indexes) |partition_index| {
                const partition_error: i16 = if (topic_info) |info|
                    if (partition_index >= 0 and partition_index < info.num_partitions)
                        @intFromEnum(ErrorCode.none)
                    else
                        @intFromEnum(ErrorCode.unknown_topic_or_partition)
                else
                    @intFromEnum(ErrorCode.unknown_topic_or_partition);

                const active_producers = if (partition_error == 0)
                    (self.collectDescribeProducerStates(topic_name, partition_index) catch return null)
                else
                    &[_]generated.describe_producers_response.DescribeProducersResponse.TopicResponse.PartitionResponse.ProducerState{};

                partition_responses[partition_init] = .{
                    .partition_index = partition_index,
                    .error_code = partition_error,
                    .error_message = null,
                    .active_producers = active_producers,
                };
                partition_init += 1;
            }

            topic_responses[topic_init] = .{
                .name = topic_req.name,
                .partitions = partition_responses,
            };
            topic_init += 1;
            transferred = true;
        }

        const resp_body = Resp{
            .throttle_time_ms = 0,
            .topics = topic_responses[0..topic_init],
        };
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const needed = rh.calcSize(resp_header_version) + resp_body.calcSize(api_version);
        var buf = self.allocator.alloc(u8, needed) catch return null;
        var wpos: usize = 0;
        rh.serialize(buf, &wpos, resp_header_version);
        resp_body.serialize(buf, &wpos, api_version);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    fn freeDescribeProducersRequest(self: *Broker, req: *generated.describe_producers_request.DescribeProducersRequest) void {
        for (req.topics) |topic| {
            if (topic.partition_indexes.len > 0) self.allocator.free(topic.partition_indexes);
        }
        if (req.topics.len > 0) self.allocator.free(req.topics);
    }

    fn freeDescribeProducersTopicResponses(self: *Broker, topics: []const generated.describe_producers_response.DescribeProducersResponse.TopicResponse) void {
        for (topics) |topic| {
            self.freeDescribeProducersPartitions(topic.partitions);
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
    }

    fn freeDescribeProducersPartitions(self: *Broker, partitions: []const generated.describe_producers_response.DescribeProducersResponse.TopicResponse.PartitionResponse) void {
        for (partitions) |partition| {
            if (partition.active_producers.len > 0) self.allocator.free(partition.active_producers);
        }
    }

    fn collectDescribeProducerStates(self: *Broker, topic: []const u8, partition_index: i32) ![]generated.describe_producers_response.DescribeProducersResponse.TopicResponse.PartitionResponse.ProducerState {
        const ProducerState = generated.describe_producers_response.DescribeProducersResponse.TopicResponse.PartitionResponse.ProducerState;

        var states = std.array_list.Managed(ProducerState).init(self.allocator);
        errdefer states.deinit();

        const partition_key = PartitionStore.hashPartitionKey(topic, partition_index);
        var seq_it = self.producer_sequences.iterator();
        while (seq_it.next()) |entry| {
            if (entry.key_ptr.partition_key != partition_key) continue;
            const state = entry.value_ptr.*;
            try states.append(.{
                .producer_id = entry.key_ptr.producer_id,
                .producer_epoch = @intCast(state.producer_epoch),
                .last_sequence = state.last_sequence,
                .last_timestamp = -1,
                .coordinator_epoch = 0,
                .current_txn_start_offset = -1,
            });
        }

        var txn_it = self.txn_coordinator.transactions.iterator();
        while (txn_it.next()) |entry| {
            const txn = entry.value_ptr;
            var matches_partition = false;
            for (txn.partitions.items) |tp| {
                if (tp.partition == partition_index and std.mem.eql(u8, tp.topic, topic)) {
                    matches_partition = true;
                    break;
                }
            }
            if (!matches_partition) continue;

            if (findDescribeProducerState(states.items, txn.producer_id)) |idx| {
                states.items[idx].producer_epoch = @intCast(txn.producer_epoch);
                states.items[idx].last_timestamp = txn.start_time_ms;
                states.items[idx].coordinator_epoch = if (txn.status == .ongoing) 0 else -1;
            } else {
                try states.append(.{
                    .producer_id = txn.producer_id,
                    .producer_epoch = @intCast(txn.producer_epoch),
                    .last_sequence = -1,
                    .last_timestamp = txn.start_time_ms,
                    .coordinator_epoch = if (txn.status == .ongoing) 0 else -1,
                    .current_txn_start_offset = -1,
                });
            }
        }

        if (states.items.len == 0) return &.{};
        return states.toOwnedSlice();
    }

    fn findDescribeProducerState(states: []const generated.describe_producers_response.DescribeProducersResponse.TopicResponse.PartitionResponse.ProducerState, producer_id: i64) ?usize {
        for (states, 0..) |state, idx| {
            if (state.producer_id == producer_id) return idx;
        }
        return null;
    }

    // ---------------------------------------------------------------
    // AlterPartitionReassignments (key 45) — flexible versions only
    // ---------------------------------------------------------------
    fn handleAlterPartitionReassignments(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = body_start;
        _ = ser.readI32(request_bytes, &pos); // timeout_ms

        // Parse topics array (compact)
        const num_topics = (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0;

        var buf = self.allocator.alloc(u8, 2048) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeCompactString(buf, &wpos, null); // error_message

        // Return per-topic responses (single-node: reassignment is no-op)
        ser.writeCompactArrayLen(buf, &wpos, num_topics);
        for (0..num_topics) |_| {
            const topic_name = (ser.readCompactString(request_bytes, &pos) catch break) orelse "";
            const num_parts = (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0;

            ser.writeCompactString(buf, &wpos, topic_name);
            ser.writeCompactArrayLen(buf, &wpos, num_parts);
            for (0..num_parts) |_| {
                const part_idx = ser.readI32(request_bytes, &pos);
                // Skip replicas array
                const n_replicas = (ser.readCompactArrayLen(request_bytes, &pos) catch 0) orelse 0;
                pos += n_replicas * 4;
                ser.skipTaggedFields(request_bytes, &pos) catch {};

                ser.writeI32(buf, &wpos, part_idx);
                ser.writeI16(buf, &wpos, 85); // NO_REASSIGNMENT_IN_PROGRESS (single-node)
                ser.writeCompactString(buf, &wpos, null);
                ser.writeEmptyTaggedFields(buf, &wpos);
            }
            ser.writeEmptyTaggedFields(buf, &wpos);
        }

        ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // ListPartitionReassignments (key 46) — flexible versions only
    // ---------------------------------------------------------------
    fn handleListPartitionReassignments(self: *Broker, request_bytes: []const u8, body_start: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        _ = request_bytes;
        _ = body_start;

        // Single-node broker: no reassignments in progress
        var buf = self.allocator.alloc(u8, 128) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeCompactString(buf, &wpos, null); // error_message
        ser.writeCompactArrayLen(buf, &wpos, 0); // 0 topics with reassignments
        ser.writeEmptyTaggedFields(buf, &wpos);
        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // Vote (key 52) — KRaft consensus
    // ---------------------------------------------------------------
    fn handleVote(self: *Broker, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        _ = api_version;
        const raft = self.raft_state orelse return self.handleNotController(req_header, resp_header_version);
        var pos = start_pos;

        // Vote (API 52) is always flexible — use compact serialization
        // Body: cluster_id (compact string), voter_id (int32), candidate_epoch (int32),
        // candidate_id (int32), last_epoch_end_offset (int64), last_epoch (int32)
        const cluster_id = ser.readCompactString(request_bytes, &pos) catch null;
        _ = cluster_id;

        // Read candidate fields directly (no topics array in Vote v0)
        const candidate_id = ser.readI32(request_bytes, &pos);
        const candidate_epoch = ser.readI32(request_bytes, &pos);
        _ = ser.readI32(request_bytes, &pos); // duplicate candidate_id in wire format
        const last_epoch_end_offset = ser.readI64(request_bytes, &pos);
        const last_epoch = ser.readI32(request_bytes, &pos);

        // Process vote request through Raft state machine
        const vote_result = raft.handleVoteRequest(
            candidate_id,
            candidate_epoch,
            @intCast(last_epoch_end_offset),
            last_epoch,
        );

        log.info("Vote request from candidate {d} epoch={d}: granted={}", .{
            candidate_id,
            candidate_epoch,
            vote_result.vote_granted,
        });

        // Build Vote response
        var buf = self.allocator.alloc(u8, 128) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);

        // Vote response body (flexible):
        ser.writeI16(buf, &wpos, if (vote_result.vote_granted) @as(i16, 0) else @as(i16, 87)); // NONE or INVALID_RECORD
        // leader_epoch
        ser.writeI32(buf, &wpos, vote_result.epoch);
        // Vote granted
        ser.writeBool(buf, &wpos, vote_result.vote_granted);
        // Tagged fields
        ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // BeginQuorumEpoch (key 53) — KRaft leader heartbeat
    // ---------------------------------------------------------------
    fn handleBeginQuorumEpoch(self: *Broker, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        const raft = self.raft_state orelse return self.handleNotController(req_header, resp_header_version);
        var pos = start_pos;

        // Parse simplified request body
        const error_code = ser.readI16(request_bytes, &pos);
        _ = error_code;
        // Skip topics array
        const topics_len = ser.readArrayLen(request_bytes, &pos) catch 0;
        _ = topics_len;
        // Leader info
        const leader_id = ser.readI32(request_bytes, &pos);
        const leader_epoch = ser.readI32(request_bytes, &pos);

        // Process through Raft state machine
        if (leader_epoch >= raft.current_epoch) {
            // Step down: if we were leader and a higher epoch arrives, we must stop writing
            if (raft.role == .leader and leader_epoch > raft.current_epoch) {
                log.info("Stepping down: received higher epoch {d} from leader {d} (was leader at epoch {d})", .{
                    leader_epoch, leader_id, raft.current_epoch,
                });
                // Fence the S3 WAL batcher to prevent stale writes
                if (self.store.s3_wal_batcher) |*batcher| {
                    batcher.fence();
                }
            }
            raft.becomeFollower(leader_epoch, leader_id);
            log.info("Acknowledged leader {d} epoch={d}", .{ leader_id, leader_epoch });
        }

        // Build response
        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code: NONE
        ser.writeI32(buf, &wpos, 0); // topics array len = 0

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // EndQuorumEpoch (key 54) — KRaft leader step-down
    // ---------------------------------------------------------------
    fn handleEndQuorumEpoch(self: *Broker, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        const raft = self.raft_state orelse return self.handleNotController(req_header, resp_header_version);
        var pos = start_pos;
        _ = request_bytes;
        _ = &pos;

        // When leader steps down, followers should start an election
        if (raft.role == .follower) {
            log.info("Leader stepped down, will start election", .{});
            // Reset election timer to trigger election soon
            raft.election_timer.reset();
        }

        // Build response
        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeI32(buf, &wpos, 0); // topics array len = 0

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DescribeQuorum (key 55) — KRaft quorum info
    // ---------------------------------------------------------------
    fn handleDescribeQuorum(self: *Broker, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        const raft = self.raft_state orelse return self.handleNotController(req_header, resp_header_version);
        var buf = self.allocator.alloc(u8, 256) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);

        // DescribeQuorum response body
        ser.writeI16(buf, &wpos, 0); // error_code
        // Topics array — one topic (__cluster_metadata)
        ser.writeI32(buf, &wpos, 1);
        ser.writeString(buf, &wpos, "__cluster_metadata");
        // Partitions array — one partition
        ser.writeI32(buf, &wpos, 1);
        // Partition 0
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeI32(buf, &wpos, 0); // partition_index
        ser.writeI32(buf, &wpos, raft.leader_id orelse -1); // leader_id
        ser.writeI32(buf, &wpos, raft.current_epoch); // leader_epoch
        ser.writeI64(buf, &wpos, @intCast(raft.log.lastOffset())); // high_watermark

        // Current voters array
        const voter_count = raft.quorumSize();
        ser.writeI32(buf, &wpos, @intCast(voter_count));
        var it = raft.voters.iterator();
        while (it.next()) |entry| {
            ser.writeI32(buf, &wpos, entry.key_ptr.*); // replica_id
            ser.writeI64(buf, &wpos, @intCast(entry.value_ptr.match_index)); // log_end_offset
        }
        // Observers array (empty)
        ser.writeI32(buf, &wpos, 0);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }
};

/// Legacy stateless handler wrapper — adapts the old function signature
/// to use a global Broker instance. Used by the current Server.
var global_broker: ?*Broker = null;

pub fn setGlobalBroker(broker: *Broker) void {
    global_broker = broker;
}

pub fn handleRequest(request_bytes: []const u8, alloc: Allocator) ?[]u8 {
    _ = alloc;
    if (global_broker) |broker| {
        return broker.handleRequest(request_bytes);
    }
    return null;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

/// Build a minimal Kafka request frame for testing.
/// Format: api_key(i16) + api_version(i16) + correlation_id(i32) + client_id(string)
fn buildTestRequest(buf: []u8, api_key: i16, api_version: i16, correlation_id: i32, header_version: i16) usize {
    var pos: usize = 0;
    ser.writeI16(buf, &pos, api_key);
    ser.writeI16(buf, &pos, api_version);
    ser.writeI32(buf, &pos, correlation_id);
    if (header_version >= 2) {
        ser.writeCompactString(buf, &pos, "test-client");
        ser.writeEmptyTaggedFields(buf, &pos); // tagged fields
    } else if (header_version >= 1) {
        ser.writeString(buf, &pos, "test-client");
    }
    return pos;
}

test "Broker.handleRequest rejects too-short request" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Empty request
    try testing.expect(broker.handleRequest("") == null);
    // Only 4 bytes — less than minimum header (8 bytes needed)
    try testing.expect(broker.handleRequest(&[_]u8{ 0, 0, 0, 0 }) == null);
    // 7 bytes — still less than minimum
    try testing.expect(broker.handleRequest(&[_]u8{ 0, 0, 0, 0, 0, 0, 0 }) == null);
}

test "Broker.handleRequest ApiVersions v0 (non-flexible)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 0, 42, 1);

    const response = broker.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Parse the response header
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 42), corr_id);

    // Parse ApiVersions response body
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);

    const api_count = (try ser.readArrayLen(response.?, &rpos)).?;
    var saw_delete_groups = false;
    var saw_incremental_alter_configs = false;
    var saw_alter_reassignments = false;
    var saw_list_reassignments = false;
    var saw_create_streams = false;
    var saw_update_group = false;
    for (0..api_count) |_| {
        const key = ser.readI16(response.?, &rpos);
        _ = ser.readI16(response.?, &rpos);
        const max_version = ser.readI16(response.?, &rpos);
        if (key == 42) {
            saw_delete_groups = true;
            try testing.expectEqual(@as(i16, 2), max_version);
        } else if (key == 44) {
            saw_incremental_alter_configs = true;
        } else if (key == 45) {
            saw_alter_reassignments = true;
        } else if (key == 46) {
            saw_list_reassignments = true;
        } else if (key == 501) {
            saw_create_streams = true;
            try testing.expectEqual(@as(i16, 1), max_version);
        } else if (key == 602) {
            saw_update_group = true;
            try testing.expectEqual(@as(i16, 0), max_version);
        }
    }
    try testing.expect(saw_delete_groups);
    try testing.expect(saw_incremental_alter_configs);
    try testing.expect(saw_alter_reassignments);
    try testing.expect(saw_list_reassignments);
    try testing.expect(saw_create_streams);
    try testing.expect(saw_update_group);
}

test "Broker.handleRequest ApiVersions v3 (flexible)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 3, 100, 2);

    const response = broker.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Response header v0 for ApiVersions — just correlation_id
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 100), corr_id);

    // Error code
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);
}

test "Broker.handleRequest Metadata (key=3, v1)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Build a metadata request for all topics (num_topics = -1)
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 3, 1, 55, 1);
    // Metadata request body: topics array with -1 (null = all topics)
    ser.writeI32(&buf, &pos, -1);

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 55), corr_id);
}

test "Broker.handleRequest unsupported API returns error response" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Use api_key=999 (nonexistent)
    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 999, 0, 77, 1);

    const response = broker.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Should still contain the correlation_id
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 77), corr_id);
}

test "Broker.handleRequest FindCoordinator (key=10, v1)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 10, 1, 200, 1);
    // FindCoordinator request body: key_type is implied as GROUP (v1 has key string only)
    ser.writeString(&buf, &pos, "my-group");

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 200), corr_id);
}

test "Broker.handleRequest Produce v0 minimal" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 0, 0, 300, 1);
    // Produce v0 body: acks(i16) + timeout(i32) + topics array
    ser.writeI16(&buf, &pos, 1); // acks = 1
    ser.writeI32(&buf, &pos, 30000); // timeout_ms
    // topics array: 1 topic
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, "test-topic");
    // partitions array: 1 partition
    ser.writeI32(&buf, &pos, 1);
    ser.writeI32(&buf, &pos, 0); // partition_index
    // records (bytes): put some dummy data
    const fake_records = "fake-record-batch-data";
    ser.writeI32(&buf, &pos, @intCast(fake_records.len));
    @memcpy(buf[pos .. pos + fake_records.len], fake_records);
    pos += fake_records.len;

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 300), corr_id);
}

test "Broker.handleRequest ListGroups (key=16)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 16, 0, 400, 1);

    const response = broker.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 400), corr_id);
}

test "Broker.handleRequest DeleteGroups v2 deletes empty group" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    const join = try broker.groups.joinGroup("delete-me", null, "consumer", null);
    try testing.expectEqual(@as(i16, 0), broker.groups.leaveGroup("delete-me", join.member_id));
    try testing.expectEqual(@as(usize, 1), broker.groups.groupCount());

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 42, 2, 4200, 2);
    ser.writeCompactArrayLen(&buf, &pos, 1);
    ser.writeCompactString(&buf, &pos, "delete-me");
    ser.writeEmptyTaggedFields(&buf, &pos);

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 4200), resp_header.correlation_id);
    try testing.expectEqual(@as(i32, 0), ser.readI32(response.?, &rpos));

    const result_count = (try ser.readCompactArrayLen(response.?, &rpos)).?;
    try testing.expectEqual(@as(usize, 1), result_count);
    const group_id = (try ser.readCompactString(response.?, &rpos)).?;
    try testing.expectEqualStrings("delete-me", group_id);
    try testing.expectEqual(@as(i16, 0), ser.readI16(response.?, &rpos));
    try testing.expectEqual(@as(usize, 0), broker.groups.groupCount());
}

test "Broker AutoMQ stream object lifecycle APIs" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    var owned_responses = std.array_list.Managed([]u8).init(testing.allocator);
    defer {
        for (owned_responses.items) |resp| testing.allocator.free(resp);
        owned_responses.deinit();
    }

    const CreateReq = generated.create_streams_request.CreateStreamsRequest;
    const CreateResp = generated.create_streams_response.CreateStreamsResponse;
    const PrepareReq = generated.prepare_s3_object_request.PrepareS3ObjectRequest;
    const PrepareResp = generated.prepare_s3_object_response.PrepareS3ObjectResponse;
    const CommitReq = generated.commit_stream_object_request.CommitStreamObjectRequest;
    const CommitResp = generated.commit_stream_object_response.CommitStreamObjectResponse;
    const TrimReq = generated.trim_streams_request.TrimStreamsRequest;
    const TrimResp = generated.trim_streams_response.TrimStreamsResponse;
    const CloseReq = generated.close_streams_request.CloseStreamsRequest;
    const CloseResp = generated.close_streams_response.CloseStreamsResponse;
    const DeleteReq = generated.delete_streams_request.DeleteStreamsRequest;
    const DeleteResp = generated.delete_streams_response.DeleteStreamsResponse;

    var buf: [2048]u8 = undefined;
    var pos = buildTestRequest(&buf, 501, 0, 5010, 2);
    const create_items = [_]CreateReq.CreateStreamRequest{.{ .node_id = 1 }};
    const create_req = CreateReq{ .node_id = 1, .node_epoch = 1, .create_stream_requests = &create_items };
    create_req.serialize(&buf, &pos, 0);
    var response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    var rpos: usize = 0;
    var create_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer create_header.deinit(testing.allocator);
    const create_resp = try CreateResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(create_resp.create_stream_responses);
    try testing.expectEqual(@as(i16, 0), create_resp.error_code);
    try testing.expectEqual(@as(usize, 1), create_resp.create_stream_responses.len);
    const stream_id = create_resp.create_stream_responses[0].stream_id;
    try testing.expect(stream_id > 0);
    try testing.expectEqual(@as(usize, 1), broker.object_manager.streamCount());

    pos = buildTestRequest(&buf, 505, 0, 5050, 2);
    const prepare_req = PrepareReq{ .node_id = 1, .prepared_count = 1, .time_to_live_in_ms = 60_000 };
    prepare_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    rpos = 0;
    var prepare_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer prepare_header.deinit(testing.allocator);
    const prepare_resp = try PrepareResp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(@as(i16, 0), prepare_resp.error_code);
    try testing.expect(prepare_resp.first_s3_object_id > 0);

    pos = buildTestRequest(&buf, 507, 1, 5070, 2);
    const source_ids = [_]i64{};
    const operations = [_]i8{};
    const commit_req = CommitReq{
        .node_id = 1,
        .node_epoch = 1,
        .object_id = prepare_resp.first_s3_object_id,
        .object_size = 128,
        .stream_id = stream_id,
        .start_offset = 0,
        .end_offset = 10,
        .source_object_ids = &source_ids,
        .stream_epoch = 1,
        .attributes = 7,
        .operations = &operations,
    };
    commit_req.serialize(&buf, &pos, 1);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    rpos = 0;
    var commit_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer commit_header.deinit(testing.allocator);
    const commit_resp = try CommitResp.deserialize(testing.allocator, response.?, &rpos, 1);
    try testing.expectEqual(@as(i16, 0), commit_resp.error_code);
    try testing.expectEqual(@as(usize, 1), broker.object_manager.getStreamObjectCount());

    pos = buildTestRequest(&buf, 512, 0, 5120, 2);
    const trim_items = [_]TrimReq.TrimStreamRequest{.{ .stream_id = stream_id, .stream_epoch = 1, .new_start_offset = 5 }};
    const trim_req = TrimReq{ .node_id = 1, .node_epoch = 1, .trim_stream_requests = &trim_items };
    trim_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    rpos = 0;
    var trim_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer trim_header.deinit(testing.allocator);
    const trim_resp = try TrimResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(trim_resp.trim_stream_responses);
    try testing.expectEqual(@as(i16, 0), trim_resp.trim_stream_responses[0].error_code);
    try testing.expectEqual(@as(u64, 5), broker.object_manager.getStream(@intCast(stream_id)).?.start_offset);

    pos = buildTestRequest(&buf, 503, 0, 5030, 2);
    const close_items = [_]CloseReq.CloseStreamRequest{.{ .stream_id = stream_id, .stream_epoch = 1 }};
    const close_req = CloseReq{ .node_id = 1, .node_epoch = 1, .close_stream_requests = &close_items };
    close_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    rpos = 0;
    var close_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer close_header.deinit(testing.allocator);
    const close_resp = try CloseResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(close_resp.close_stream_responses);
    try testing.expectEqual(@as(i16, 0), close_resp.close_stream_responses[0].error_code);

    pos = buildTestRequest(&buf, 504, 0, 5040, 2);
    const delete_items = [_]DeleteReq.DeleteStreamRequest{.{ .stream_id = stream_id, .stream_epoch = 1 }};
    const delete_req = DeleteReq{ .node_id = 1, .node_epoch = 1, .delete_stream_requests = &delete_items };
    delete_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    rpos = 0;
    var delete_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer delete_header.deinit(testing.allocator);
    const delete_resp = try DeleteResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(delete_resp.delete_stream_responses);
    try testing.expectEqual(@as(i16, 0), delete_resp.delete_stream_responses[0].error_code);
    try testing.expectEqual(@as(usize, 0), broker.object_manager.streamCount());
    try testing.expectEqual(@as(usize, 0), broker.object_manager.getStreamObjectCount());
}

test "Broker AutoMQ KV APIs round-trip" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    var owned_responses = std.array_list.Managed([]u8).init(testing.allocator);
    defer {
        for (owned_responses.items) |resp| testing.allocator.free(resp);
        owned_responses.deinit();
    }

    const PutReq = generated.put_k_vs_request.PutKVsRequest;
    const PutResp = generated.put_k_vs_response.PutKVsResponse;
    const GetReq = generated.get_k_vs_request.GetKVsRequest;
    const GetResp = generated.get_k_vs_response.GetKVsResponse;
    const DeleteReq = generated.delete_k_vs_request.DeleteKVsRequest;
    const DeleteResp = generated.delete_k_vs_response.DeleteKVsResponse;

    var buf: [1024]u8 = undefined;
    var pos = buildTestRequest(&buf, 510, 0, 5100, 2);
    const put_items = [_]PutReq.PutKVRequest{.{ .key = "alpha", .value = "beta", .overwrite = false }};
    const put_req = PutReq{ .put_kv_requests = &put_items };
    put_req.serialize(&buf, &pos, 0);
    var response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    var rpos: usize = 0;
    var put_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer put_header.deinit(testing.allocator);
    const put_resp = try PutResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(put_resp.put_kv_responses);
    try testing.expectEqual(@as(i16, 0), put_resp.put_kv_responses[0].error_code);

    pos = buildTestRequest(&buf, 509, 0, 5090, 2);
    const get_items = [_]GetReq.GetKVRequest{.{ .key = "alpha" }};
    const get_req = GetReq{ .get_key_requests = &get_items };
    get_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    rpos = 0;
    var get_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer get_header.deinit(testing.allocator);
    const get_resp = try GetResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(get_resp.get_kv_responses);
    try testing.expectEqual(@as(i16, 0), get_resp.get_kv_responses[0].error_code);
    try testing.expectEqualStrings("beta", get_resp.get_kv_responses[0].value.?);

    pos = buildTestRequest(&buf, 511, 0, 5110, 2);
    const delete_items = [_]DeleteReq.DeleteKVRequest{.{ .key = "alpha" }};
    const delete_req = DeleteReq{ .delete_kv_requests = &delete_items };
    delete_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);

    rpos = 0;
    var delete_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer delete_header.deinit(testing.allocator);
    const delete_resp = try DeleteResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(delete_resp.delete_kv_responses);
    try testing.expectEqual(@as(i16, 0), delete_resp.delete_kv_responses[0].error_code);
    try testing.expectEqualStrings("beta", delete_resp.delete_kv_responses[0].value.?);
    try testing.expectEqual(@as(u32, 0), broker.auto_mq_kvs.count());
}

test "Broker AutoMQ node license and manifest APIs" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    var owned_responses = std.array_list.Managed([]u8).init(testing.allocator);
    defer {
        for (owned_responses.items) |resp| testing.allocator.free(resp);
        owned_responses.deinit();
    }

    const RegisterReq = generated.automq_register_node_request.AutomqRegisterNodeRequest;
    const RegisterResp = generated.automq_register_node_response.AutomqRegisterNodeResponse;
    const GetNodesReq = generated.automq_get_nodes_request.AutomqGetNodesRequest;
    const GetNodesResp = generated.automq_get_nodes_response.AutomqGetNodesResponse;
    const NextNodeReq = generated.get_next_node_id_request.GetNextNodeIdRequest;
    const NextNodeResp = generated.get_next_node_id_response.GetNextNodeIdResponse;
    const UpdateLicenseReq = generated.update_license_request.UpdateLicenseRequest;
    const UpdateLicenseResp = generated.update_license_response.UpdateLicenseResponse;
    const DescribeLicenseReq = generated.describe_license_request.DescribeLicenseRequest;
    const DescribeLicenseResp = generated.describe_license_response.DescribeLicenseResponse;
    const ManifestReq = generated.export_cluster_manifest_request.ExportClusterManifestRequest;
    const ManifestResp = generated.export_cluster_manifest_response.ExportClusterManifestResponse;

    var buf: [2048]u8 = undefined;
    var pos = buildTestRequest(&buf, 513, 0, 5130, 2);
    const register_req = RegisterReq{ .node_id = 7, .node_epoch = 3, .wal_config = "wal://node-7" };
    register_req.serialize(&buf, &pos, 0);
    var response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    var rpos: usize = 0;
    var register_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer register_header.deinit(testing.allocator);
    const register_resp = try RegisterResp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(@as(i16, 0), register_resp.error_code);

    pos = buildTestRequest(&buf, 514, 0, 5140, 2);
    const node_ids = [_]i32{7};
    const get_nodes_req = GetNodesReq{ .node_ids = &node_ids };
    get_nodes_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var get_nodes_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer get_nodes_header.deinit(testing.allocator);
    const get_nodes_resp = try GetNodesResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(get_nodes_resp.nodes);
    try testing.expectEqual(@as(usize, 1), get_nodes_resp.nodes.len);
    try testing.expectEqual(@as(i32, 7), get_nodes_resp.nodes[0].node_id);
    try testing.expectEqualStrings("wal://node-7", get_nodes_resp.nodes[0].wal_config.?);

    pos = buildTestRequest(&buf, 600, 0, 6000, 2);
    const next_node_req = NextNodeReq{ .cluster_id = "zmq-cluster" };
    next_node_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var next_node_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer next_node_header.deinit(testing.allocator);
    const next_node_resp = try NextNodeResp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(@as(i32, 8), next_node_resp.node_id);

    pos = buildTestRequest(&buf, 517, 0, 5170, 2);
    const update_license_req = UpdateLicenseReq{ .license = "test-license" };
    update_license_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var update_license_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer update_license_header.deinit(testing.allocator);
    const update_license_resp = try UpdateLicenseResp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(@as(i16, 0), update_license_resp.error_code);

    pos = buildTestRequest(&buf, 518, 0, 5180, 2);
    const describe_license_req = DescribeLicenseReq{};
    describe_license_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var describe_license_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer describe_license_header.deinit(testing.allocator);
    const describe_license_resp = try DescribeLicenseResp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqualStrings("test-license", describe_license_resp.license.?);

    pos = buildTestRequest(&buf, 519, 0, 5190, 2);
    const manifest_req = ManifestReq{};
    manifest_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var manifest_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer manifest_header.deinit(testing.allocator);
    const manifest_resp = try ManifestResp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expect(std.mem.indexOf(u8, manifest_resp.manifest.?, "\"cluster_id\":\"zmq-cluster\"") != null);
}

test "Broker AutoMQ router snapshot describe and group APIs" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    var owned_responses = std.array_list.Managed([]u8).init(testing.allocator);
    defer {
        for (owned_responses.items) |resp| testing.allocator.free(resp);
        owned_responses.deinit();
    }

    const OpenReq = generated.open_streams_request.OpenStreamsRequest;
    const OpenResp = generated.open_streams_response.OpenStreamsResponse;
    const OpeningReq = generated.get_opening_streams_request.GetOpeningStreamsRequest;
    const OpeningResp = generated.get_opening_streams_response.GetOpeningStreamsResponse;
    const CommitSsoReq = generated.commit_stream_set_object_request.CommitStreamSetObjectRequest;
    const CommitSsoResp = generated.commit_stream_set_object_response.CommitStreamSetObjectResponse;
    const DescribeReq = generated.describe_streams_request.DescribeStreamsRequest;
    const DescribeResp = generated.describe_streams_response.DescribeStreamsResponse;
    const ZoneReq = generated.automq_zone_router_request.AutomqZoneRouterRequest;
    const ZoneResp = generated.automq_zone_router_response.AutomqZoneRouterResponse;
    const SnapshotReq = generated.automq_get_partition_snapshot_request.AutomqGetPartitionSnapshotRequest;
    const SnapshotResp = generated.automq_get_partition_snapshot_response.AutomqGetPartitionSnapshotResponse;
    const UpdateGroupReq = generated.automq_update_group_request.AutomqUpdateGroupRequest;
    const UpdateGroupResp = generated.automq_update_group_response.AutomqUpdateGroupResponse;

    try testing.expect(broker.ensureTopic("snap-topic"));
    const stream = try broker.object_manager.createStream(1);
    const stream_id: i64 = @intCast(stream.stream_id);

    var buf: [4096]u8 = undefined;
    var pos = buildTestRequest(&buf, 502, 1, 5020, 2);
    const open_items = [_]OpenReq.OpenStreamRequest{.{ .stream_id = stream_id, .stream_epoch = 1 }};
    const open_req = OpenReq{ .node_id = 1, .node_epoch = 1, .open_stream_requests = &open_items };
    open_req.serialize(&buf, &pos, 1);
    var response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    var rpos: usize = 0;
    var open_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer open_header.deinit(testing.allocator);
    const open_resp = try OpenResp.deserialize(testing.allocator, response.?, &rpos, 1);
    defer testing.allocator.free(open_resp.open_stream_responses);
    try testing.expectEqual(@as(i16, 0), open_resp.open_stream_responses[0].error_code);

    pos = buildTestRequest(&buf, 508, 0, 5080, 2);
    const opening_req = OpeningReq{ .node_id = 1, .node_epoch = 1, .failover_mode = false };
    opening_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var opening_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer opening_header.deinit(testing.allocator);
    const opening_resp = try OpeningResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(opening_resp.stream_metadata_list);
    try testing.expect(opening_resp.stream_metadata_list.len >= 1);

    const sso_object_id = broker.object_manager.prepareObject();
    pos = buildTestRequest(&buf, 506, 1, 5060, 2);
    const ranges = [_]CommitSsoReq.ObjectStreamRange{.{
        .stream_id = stream_id,
        .stream_epoch = 1,
        .start_offset = 0,
        .end_offset = 10,
    }};
    const stream_objects = [_]CommitSsoReq.StreamObject{};
    const compacted = [_]i64{};
    const commit_sso_req = CommitSsoReq{
        .node_id = 1,
        .node_epoch = 1,
        .object_id = @intCast(sso_object_id),
        .order_id = 1,
        .object_size = 512,
        .object_stream_ranges = &ranges,
        .stream_objects = &stream_objects,
        .compacted_object_ids = &compacted,
        .failover_mode = false,
        .attributes = 9,
    };
    commit_sso_req.serialize(&buf, &pos, 1);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var commit_sso_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer commit_sso_header.deinit(testing.allocator);
    const commit_sso_resp = try CommitSsoResp.deserialize(testing.allocator, response.?, &rpos, 1);
    try testing.expectEqual(@as(i16, 0), commit_sso_resp.error_code);
    try testing.expectEqual(@as(usize, 1), broker.object_manager.getStreamSetObjectCount());

    pos = buildTestRequest(&buf, 601, 0, 6010, 2);
    const describe_req = DescribeReq{ .node_id = 1, .stream_id = stream_id };
    describe_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var describe_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer describe_header.deinit(testing.allocator);
    const describe_resp = try DescribeResp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer testing.allocator.free(describe_resp.stream_metadata_list);
    try testing.expectEqual(@as(usize, 1), describe_resp.stream_metadata_list.len);
    try testing.expectEqual(stream_id, describe_resp.stream_metadata_list[0].stream_id);

    pos = buildTestRequest(&buf, 515, 1, 5150, 2);
    const zone_req = ZoneReq{ .metadata = "route-data", .route_epoch = 4, .version = 1 };
    zone_req.serialize(&buf, &pos, 1);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var zone_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer zone_header.deinit(testing.allocator);
    const zone_resp = try ZoneResp.deserialize(testing.allocator, response.?, &rpos, 1);
    defer testing.allocator.free(zone_resp.responses);
    try testing.expectEqualStrings("route-data", zone_resp.responses[0].data.?);

    pos = buildTestRequest(&buf, 516, 2, 5160, 2);
    const snapshot_req = SnapshotReq{ .session_id = 11, .session_epoch = 2, .request_commit = false, .version = 2 };
    snapshot_req.serialize(&buf, &pos, 2);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var snapshot_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer snapshot_header.deinit(testing.allocator);
    const snapshot_resp = try SnapshotResp.deserialize(testing.allocator, response.?, &rpos, 2);
    defer {
        for (snapshot_resp.topics) |topic| {
            for (topic.partitions) |partition| {
                if (partition.stream_metadata.len > 0) testing.allocator.free(partition.stream_metadata);
            }
            if (topic.partitions.len > 0) testing.allocator.free(topic.partitions);
        }
        if (snapshot_resp.topics.len > 0) testing.allocator.free(snapshot_resp.topics);
    }
    try testing.expectEqual(@as(i16, 0), snapshot_resp.error_code);
    try testing.expect(snapshot_resp.topics.len >= 1);

    pos = buildTestRequest(&buf, 602, 0, 6020, 2);
    const update_group_req = UpdateGroupReq{ .link_id = "link-a", .group_id = "group-a", .promoted = true };
    update_group_req.serialize(&buf, &pos, 0);
    response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    try owned_responses.append(response.?);
    rpos = 0;
    var update_group_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer update_group_header.deinit(testing.allocator);
    const update_group_resp = try UpdateGroupResp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(@as(i16, 0), update_group_resp.error_code);
    try testing.expectEqual(@as(u32, 1), broker.auto_mq_group_promotions.count());
}

test "Broker restores AutoMQ metadata after restart" {
    const fs = @import("fs_compat");
    const tmp_dir = "/tmp/zmq-automq-metadata-restart-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    try fs.makeDirAbsolute(tmp_dir);
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    const PutReq = generated.put_k_vs_request.PutKVsRequest;
    const RegisterReq = generated.automq_register_node_request.AutomqRegisterNodeRequest;
    const ZoneReq = generated.automq_zone_router_request.AutomqZoneRouterRequest;
    const UpdateLicenseReq = generated.update_license_request.UpdateLicenseRequest;
    const NextNodeReq = generated.get_next_node_id_request.GetNextNodeIdRequest;
    const UpdateGroupReq = generated.automq_update_group_request.AutomqUpdateGroupRequest;

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        var owned_responses = std.array_list.Managed([]u8).init(testing.allocator);
        defer {
            for (owned_responses.items) |resp| testing.allocator.free(resp);
            owned_responses.deinit();
        }

        var buf: [2048]u8 = undefined;
        var pos = buildTestRequest(&buf, 510, 0, 5100, 2);
        const put_items = [_]PutReq.PutKVRequest{.{ .key = "alpha", .value = "beta", .overwrite = true }};
        const put_req = PutReq{ .put_kv_requests = &put_items };
        put_req.serialize(&buf, &pos, 0);
        var response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        pos = buildTestRequest(&buf, 513, 0, 5130, 2);
        const register_req = RegisterReq{ .node_id = 7, .node_epoch = 3, .wal_config = "wal://node-7" };
        register_req.serialize(&buf, &pos, 0);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        pos = buildTestRequest(&buf, 515, 1, 5150, 2);
        const zone_req = ZoneReq{ .metadata = "route-data", .route_epoch = 4, .version = 1 };
        zone_req.serialize(&buf, &pos, 1);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        pos = buildTestRequest(&buf, 517, 0, 5170, 2);
        const update_license_req = UpdateLicenseReq{ .license = "test-license" };
        update_license_req.serialize(&buf, &pos, 0);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        pos = buildTestRequest(&buf, 600, 0, 6000, 2);
        const next_node_req = NextNodeReq{ .cluster_id = "zmq-cluster" };
        next_node_req.serialize(&buf, &pos, 0);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        pos = buildTestRequest(&buf, 602, 0, 6020, 2);
        const update_group_req = UpdateGroupReq{ .link_id = "link-a", .group_id = "group-a", .promoted = true };
        update_group_req.serialize(&buf, &pos, 0);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);
    }

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        try testing.expectEqualStrings("beta", broker.auto_mq_kvs.get("alpha").?);
        const node = broker.auto_mq_nodes.get(7).?;
        try testing.expectEqual(@as(i64, 3), node.node_epoch);
        try testing.expectEqualStrings("wal://node-7", node.wal_config);
        try testing.expectEqual(@as(i32, 9), broker.auto_mq_next_node_id);
        try testing.expectEqualStrings("test-license", broker.auto_mq_license.?);
        try testing.expectEqualStrings("route-data", broker.auto_mq_zone_router_metadata.?);
        try testing.expectEqual(@as(i64, 4), broker.auto_mq_zone_router_epoch);
        const promotion = broker.auto_mq_group_promotions.get("group-a").?;
        try testing.expectEqualStrings("link-a", promotion.link_id);
        try testing.expect(promotion.promoted);
    }
}

test "Broker restores AutoMQ stream object snapshot after restart" {
    const fs = @import("fs_compat");
    const tmp_dir = "/tmp/zmq-object-manager-restart-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    try fs.makeDirAbsolute(tmp_dir);
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    const CreateReq = generated.create_streams_request.CreateStreamsRequest;
    const CreateResp = generated.create_streams_response.CreateStreamsResponse;
    const PrepareReq = generated.prepare_s3_object_request.PrepareS3ObjectRequest;
    const PrepareResp = generated.prepare_s3_object_response.PrepareS3ObjectResponse;
    const CommitReq = generated.commit_stream_object_request.CommitStreamObjectRequest;
    const TrimReq = generated.trim_streams_request.TrimStreamsRequest;

    var stream_id: i64 = 0;
    var committed_object_id: i64 = 0;
    var prepared_only_object_id: i64 = 0;

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        var owned_responses = std.array_list.Managed([]u8).init(testing.allocator);
        defer {
            for (owned_responses.items) |resp| testing.allocator.free(resp);
            owned_responses.deinit();
        }

        var buf: [4096]u8 = undefined;
        var pos = buildTestRequest(&buf, 501, 0, 5010, 2);
        const create_items = [_]CreateReq.CreateStreamRequest{.{ .node_id = 1 }};
        const create_req = CreateReq{ .node_id = 1, .node_epoch = 1, .create_stream_requests = &create_items };
        create_req.serialize(&buf, &pos, 0);
        var response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        var rpos: usize = 0;
        var create_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
        defer create_header.deinit(testing.allocator);
        const create_resp = try CreateResp.deserialize(testing.allocator, response.?, &rpos, 0);
        defer testing.allocator.free(create_resp.create_stream_responses);
        try testing.expectEqual(@as(i16, 0), create_resp.error_code);
        stream_id = create_resp.create_stream_responses[0].stream_id;

        pos = buildTestRequest(&buf, 505, 0, 5050, 2);
        const prepare_req = PrepareReq{ .node_id = 1, .prepared_count = 1, .time_to_live_in_ms = 60_000 };
        prepare_req.serialize(&buf, &pos, 0);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        rpos = 0;
        var prepare_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
        defer prepare_header.deinit(testing.allocator);
        const prepare_resp = try PrepareResp.deserialize(testing.allocator, response.?, &rpos, 0);
        try testing.expectEqual(@as(i16, 0), prepare_resp.error_code);
        committed_object_id = prepare_resp.first_s3_object_id;

        pos = buildTestRequest(&buf, 507, 1, 5070, 2);
        const source_ids = [_]i64{};
        const operations = [_]i8{};
        const commit_req = CommitReq{
            .node_id = 1,
            .node_epoch = 1,
            .object_id = committed_object_id,
            .object_size = 128,
            .stream_id = stream_id,
            .start_offset = 0,
            .end_offset = 10,
            .source_object_ids = &source_ids,
            .stream_epoch = 1,
            .attributes = 0,
            .operations = &operations,
        };
        commit_req.serialize(&buf, &pos, 1);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        pos = buildTestRequest(&buf, 505, 0, 5051, 2);
        prepare_req.serialize(&buf, &pos, 0);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);

        rpos = 0;
        var prepare_only_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
        defer prepare_only_header.deinit(testing.allocator);
        const prepare_only_resp = try PrepareResp.deserialize(testing.allocator, response.?, &rpos, 0);
        try testing.expectEqual(@as(i16, 0), prepare_only_resp.error_code);
        prepared_only_object_id = prepare_only_resp.first_s3_object_id;

        pos = buildTestRequest(&buf, 512, 0, 5120, 2);
        const trim_items = [_]TrimReq.TrimStreamRequest{.{ .stream_id = stream_id, .stream_epoch = 1, .new_start_offset = 5 }};
        const trim_req = TrimReq{ .node_id = 1, .node_epoch = 1, .trim_stream_requests = &trim_items };
        trim_req.serialize(&buf, &pos, 0);
        response = broker.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        try owned_responses.append(response.?);
    }

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        const restored_stream = broker.object_manager.getStream(@intCast(stream_id)).?;
        try testing.expectEqual(@as(u64, 5), restored_stream.start_offset);
        try testing.expectEqual(@as(u64, 10), restored_stream.end_offset);

        const restored_so = broker.object_manager.stream_objects.get(@intCast(committed_object_id)).?;
        try testing.expectEqual(@as(u64, @intCast(stream_id)), restored_so.stream_id);
        try testing.expectEqual(@as(u64, 0), restored_so.start_offset);
        try testing.expectEqual(@as(u64, 10), restored_so.end_offset);
        try testing.expect(broker.object_manager.prepared_registry.contains(@intCast(prepared_only_object_id)));
        try testing.expect(broker.object_manager.next_object_id > @as(u64, @intCast(prepared_only_object_id)));

        const objects = try broker.object_manager.getObjects(@intCast(stream_id), 0, 10, 10);
        defer testing.allocator.free(objects);
        try testing.expectEqual(@as(usize, 1), objects.len);
        try testing.expectEqual(@as(u64, @intCast(committed_object_id)), objects[0].object_id);
    }
}

test "Broker restores partition state after restart" {
    const fs = @import("fs_compat");
    const tmp_dir = "/tmp/zmq-partition-state-restart-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    try fs.makeDirAbsolute(tmp_dir);
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        try testing.expect(broker.ensureTopic("pstate-topic"));
        _ = try broker.store.produce("pstate-topic", 0, "record-0");
        _ = try broker.store.produce("pstate-topic", 0, "record-1");

        const pkey = try std.fmt.allocPrint(testing.allocator, "{s}-{d}", .{ "pstate-topic", 0 });
        defer testing.allocator.free(pkey);
        const state = broker.store.partitions.getPtr(pkey).?;
        state.log_start_offset = 1;
        state.high_watermark = 2;
        state.first_unstable_txn_offset = 1;
        state.last_stable_offset = 1;

        const stream_id = PartitionStore.hashPartitionKey("pstate-topic", 0);
        broker.object_manager.trimStream(stream_id, 1) catch {};
        broker.persistPartitionStates();
        broker.persistObjectManagerSnapshot();
    }

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        const pkey = try std.fmt.allocPrint(testing.allocator, "{s}-{d}", .{ "pstate-topic", 0 });
        defer testing.allocator.free(pkey);
        const state = broker.store.partitions.get(pkey).?;
        try testing.expectEqual(@as(u64, 2), state.next_offset);
        try testing.expectEqual(@as(u64, 1), state.log_start_offset);
        try testing.expectEqual(@as(u64, 2), state.high_watermark);
        try testing.expectEqual(@as(u64, 1), state.last_stable_offset);
        try testing.expectEqual(@as(u64, 1), state.first_unstable_txn_offset.?);

        const stream_id = PartitionStore.hashPartitionKey("pstate-topic", 0);
        const stream = broker.object_manager.getStream(stream_id).?;
        try testing.expectEqual(@as(u64, 1), stream.start_offset);
        try testing.expectEqual(@as(u64, 2), stream.end_offset);
    }
}

test "Broker clamps invalid restored partition state invariants" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    try testing.expect(broker.ensureTopic("clamp-topic"));

    const topic = try testing.allocator.dupe(u8, "clamp-topic");
    defer testing.allocator.free(topic);
    const entries = [_]MetadataPersistence.PartitionStateEntry{.{
        .topic = topic,
        .partition_id = 0,
        .next_offset = 10,
        .log_start_offset = 8,
        .high_watermark = 3,
        .last_stable_offset = 2,
        .first_unstable_txn_offset = 1,
    }};

    try broker.restorePartitionStates(&entries);

    const pkey = try std.fmt.allocPrint(testing.allocator, "{s}-{d}", .{ "clamp-topic", 0 });
    defer testing.allocator.free(pkey);
    const state = broker.store.partitions.get(pkey).?;
    try testing.expectEqual(@as(u64, 10), state.next_offset);
    try testing.expectEqual(@as(u64, 8), state.log_start_offset);
    try testing.expectEqual(@as(u64, 8), state.high_watermark);
    try testing.expectEqual(@as(u64, 8), state.last_stable_offset);
    try testing.expectEqual(@as(u64, 8), state.first_unstable_txn_offset.?);

    const stream_id = PartitionStore.hashPartitionKey("clamp-topic", 0);
    const stream = broker.object_manager.getStream(stream_id).?;
    try testing.expectEqual(@as(u64, 8), stream.start_offset);
    try testing.expectEqual(@as(u64, 10), stream.end_offset);
}

test "Broker fetches filesystem WAL records after restart" {
    const fs = @import("fs_compat");
    const tmp_dir = "/tmp/zmq-broker-wal-replay-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    try fs.makeDirAbsolute(tmp_dir);
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        try testing.expect(broker.ensureTopic("wal-replay-topic"));
        _ = try broker.store.produce("wal-replay-topic", 0, "x");
        _ = try broker.store.produce("wal-replay-topic", 0, "y");
        try broker.store.sync();
        broker.persistPartitionStates();
    }

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();

        const result = try broker.store.fetch("wal-replay-topic", 0, 0, 1024);
        defer if (result.records.len > 0) testing.allocator.free(@constCast(result.records));
        try testing.expectEqual(@as(i16, 0), result.error_code);
        try testing.expectEqualStrings("xy", result.records);
    }
}

test "Broker rebuilds ObjectManager from S3 WAL objects when snapshot is missing" {
    var mock_s3 = storage.MockS3.init(testing.allocator);
    defer mock_s3.deinit();
    var s3_storage = storage.S3Storage.initMock(testing.allocator, &mock_s3);

    const stream_id = PartitionStore.hashPartitionKey("broker-s3-recover-topic", 0);
    {
        var batcher = storage.wal.S3WalBatcher.init(testing.allocator);
        defer batcher.deinit();
        try batcher.append(stream_id, 0, "a");
        try batcher.append(stream_id, 1, "b");
        try testing.expect(batcher.flushNow(&s3_storage));
    }

    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    broker.store.s3_storage = s3_storage;

    try broker.open();

    try testing.expectEqual(@as(usize, 1), broker.object_manager.getStreamSetObjectCount());
    try testing.expectEqual(@as(u64, 2), broker.object_manager.getStream(stream_id).?.end_offset);
}

test "Broker open fails on unreadable S3 WAL object without snapshot" {
    var mock_s3 = storage.MockS3.init(testing.allocator);
    defer mock_s3.deinit();
    var s3_storage = storage.S3Storage.initMock(testing.allocator, &mock_s3);
    try s3_storage.putObject("wal/bad-object", "not-an-object-writer-payload");

    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    broker.store.s3_storage = s3_storage;

    try testing.expectError(error.InvalidMagic, broker.open());
}

test "Broker repairs partition offsets from recovered S3 WAL objects" {
    const fs = @import("fs_compat");
    const tmp_dir = "/tmp/zmq-broker-s3-partition-repair-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    try fs.makeDirAbsolute(tmp_dir);
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var mock_s3 = storage.MockS3.init(testing.allocator);
    defer mock_s3.deinit();
    var s3_storage = storage.S3Storage.initMock(testing.allocator, &mock_s3);

    const stream_id = PartitionStore.hashPartitionKey("broker-s3-repair-topic", 0);
    {
        var batcher = storage.wal.S3WalBatcher.init(testing.allocator);
        defer batcher.deinit();
        try batcher.append(stream_id, 0, "a");
        try batcher.append(stream_id, 1, "b");
        try testing.expect(batcher.flushNow(&s3_storage));
    }

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        try broker.open();
        try testing.expect(broker.ensureTopic("broker-s3-repair-topic"));
    }

    const objects_snapshot_path = try std.fmt.allocPrint(testing.allocator, "{s}/objects.snapshot", .{tmp_dir});
    defer testing.allocator.free(objects_snapshot_path);
    fs.deleteFileAbsolute(objects_snapshot_path) catch {};

    const partition_state_path = try std.fmt.allocPrint(testing.allocator, "{s}/partition_state.meta", .{tmp_dir});
    defer testing.allocator.free(partition_state_path);
    fs.deleteFileAbsolute(partition_state_path) catch {};

    {
        var broker = Broker.initWithConfig(testing.allocator, 1, 9092, .{ .data_dir = tmp_dir });
        defer broker.deinit();
        broker.store.s3_storage = s3_storage;
        try broker.open();

        const pkey = try std.fmt.allocPrint(testing.allocator, "{s}-{d}", .{ "broker-s3-repair-topic", 0 });
        defer testing.allocator.free(pkey);
        const state = broker.store.partitions.get(pkey).?;
        try testing.expectEqual(@as(u64, 2), state.next_offset);
        try testing.expectEqual(@as(u64, 2), state.high_watermark);
        try testing.expectEqual(@as(u64, 2), state.last_stable_offset);

        const result = try broker.store.fetch("broker-s3-repair-topic", 0, 0, 1024);
        defer if (result.records.len > 0) testing.allocator.free(@constCast(result.records));
        try testing.expectEqual(@as(i16, 0), result.error_code);
        try testing.expectEqualStrings("ab", result.records);
    }
}

test "Broker.handleRequest InitProducerId (key=22, v0)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 22, 0, 500, 1);
    // InitProducerId v0 body: transactional_id(string) + timeout(i32)
    ser.writeString(&buf, &pos, null); // null transactional_id (non-transactional)
    ser.writeI32(&buf, &pos, 60000); // transaction_timeout_ms

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 500), corr_id);
}

test "Broker.isVersionSupported" {
    // ApiVersions is always supported (special case)
    // Produce: v0-v11
    try testing.expect(Broker.isVersionSupported(0, 0));
    try testing.expect(Broker.isVersionSupported(0, 11));
    try testing.expect(!Broker.isVersionSupported(0, 12));
    // Unknown API
    try testing.expect(!Broker.isVersionSupported(999, 0));
    // Fetch: v0-v17
    try testing.expect(Broker.isVersionSupported(1, 0));
    try testing.expect(Broker.isVersionSupported(1, 17));
    try testing.expect(!Broker.isVersionSupported(1, 18));
    try testing.expect(!Broker.isVersionSupported(4, 0));
    try testing.expect(!Broker.isVersionSupported(5, 0));
    try testing.expect(!Broker.isVersionSupported(6, 0));
    try testing.expect(!Broker.isVersionSupported(7, 0));
    try testing.expect(Broker.isVersionSupported(42, 2));
    try testing.expect(!Broker.isVersionSupported(42, 3));
    try testing.expect(Broker.isVersionSupported(44, 1));
    try testing.expect(Broker.isVersionSupported(45, 0));
    try testing.expect(Broker.isVersionSupported(46, 0));
}

test "Broker.ensureTopic auto-create" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    try testing.expect(!broker.topics.contains("new-topic"));
    try testing.expect(broker.ensureTopic("new-topic"));
    try testing.expect(broker.topics.contains("new-topic"));

    // Should not create if auto_create_topics is false
    broker.auto_create_topics = false;
    try testing.expect(!broker.ensureTopic("another-topic"));
}

test "Broker.ensureTopic returns true for existing topic" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    _ = broker.ensureTopic("existing");
    try testing.expect(broker.ensureTopic("existing"));
}

test "Broker correlation ID preservation across APIs" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Test that every API handler correctly echoes back the correlation_id
    const api_keys = [_]i16{ 18, 16, 35, 60, 61, 55 };
    for (api_keys) |api_key| {
        const version: i16 = 0;
        const header_ver = header_mod.requestHeaderVersion(api_key, version);
        var buf: [256]u8 = undefined;
        var req_len = buildTestRequest(&buf, api_key, version, 12345, header_ver);
        if (api_key == 35) {
            const req = generated.describe_log_dirs_request.DescribeLogDirsRequest{};
            req.serialize(&buf, &req_len, version);
        } else if (api_key == 61) {
            const req = generated.describe_producers_request.DescribeProducersRequest{};
            req.serialize(&buf, &req_len, version);
        }

        if (broker.handleRequest(buf[0..req_len])) |response| {
            defer testing.allocator.free(response);
            var rpos: usize = 0;
            const corr_id = ser.readI32(response, &rpos);
            try testing.expectEqual(@as(i32, 12345), corr_id);
        }
    }
}

test "Broker.handleRequest DescribeLogDirs scopes flexible response to requested topics" {
    const Req = generated.describe_log_dirs_request.DescribeLogDirsRequest;
    const Resp = generated.describe_log_dirs_response.DescribeLogDirsResponse;

    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    try testing.expect(broker.ensureTopic("log-topic"));
    try testing.expect(broker.ensureTopic("other-topic"));
    _ = try broker.store.produce("log-topic", 0, "abc");
    _ = try broker.store.produce("other-topic", 0, "xyz");

    const log_partitions = [_]i32{ 0, 2 };
    const missing_partitions = [_]i32{0};
    const topics = [_]Req.DescribableLogDirTopic{
        .{ .topic = "log-topic", .partitions = &log_partitions },
        .{ .topic = "missing-topic", .partitions = &missing_partitions },
    };
    const req = Req{ .topics = &topics };

    var buf: [1024]u8 = undefined;
    var pos = buildTestRequest(&buf, 35, 2, 3502, header_mod.requestHeaderVersion(35, 2));
    req.serialize(&buf, &pos, 2);

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var response_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(35, 2));
    defer response_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 3502), response_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 2);
    defer {
        for (resp.results) |result| {
            for (result.topics) |topic| {
                if (topic.partitions.len > 0) testing.allocator.free(topic.partitions);
            }
            if (result.topics.len > 0) testing.allocator.free(result.topics);
        }
        if (resp.results.len > 0) testing.allocator.free(resp.results);
    }

    try testing.expectEqual(@as(usize, 1), resp.results.len);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.none)), resp.results[0].error_code);
    try testing.expectEqual(@as(usize, 2), resp.results[0].topics.len);
    try testing.expectEqualStrings("log-topic", resp.results[0].topics[0].name.?);
    try testing.expectEqual(@as(usize, 1), resp.results[0].topics[0].partitions.len);
    try testing.expectEqual(@as(i32, 0), resp.results[0].topics[0].partitions[0].partition_index);
    try testing.expectEqual(@as(i64, 3), resp.results[0].topics[0].partitions[0].partition_size);
    try testing.expectEqualStrings("missing-topic", resp.results[0].topics[1].name.?);
    try testing.expectEqual(@as(usize, 0), resp.results[0].topics[1].partitions.len);
}

test "Broker.handleRequest DescribeLogDirs rejects truncated request" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [128]u8 = undefined;
    const req_len = buildTestRequest(&buf, 35, 2, 3503, header_mod.requestHeaderVersion(35, 2));
    try testing.expect(broker.handleRequest(buf[0..req_len]) == null);
}

test "Broker.handleRequest ElectLeaders returns requested partition results" {
    const Req = generated.elect_leaders_request.ElectLeadersRequest;
    const Resp = generated.elect_leaders_response.ElectLeadersResponse;

    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    try testing.expect(broker.ensureTopic("elect-topic"));

    const partitions = [_]i32{ 0, 5 };
    const topics = [_]Req.TopicPartitions{.{
        .topic = "elect-topic",
        .partitions = &partitions,
    }};
    const req = Req{
        .election_type = 0,
        .topic_partitions = &topics,
        .timeout_ms = 1000,
    };

    var buf: [1024]u8 = undefined;
    var pos = buildTestRequest(&buf, 43, 2, 4300, 2);
    req.serialize(&buf, &pos, 2);

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var response_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, 1);
    defer response_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 4300), response_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 2);
    defer {
        for (resp.replica_election_results) |result| {
            if (result.partition_result.len > 0) testing.allocator.free(result.partition_result);
        }
        if (resp.replica_election_results.len > 0) testing.allocator.free(resp.replica_election_results);
    }

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.replica_election_results.len);
    try testing.expectEqualStrings("elect-topic", resp.replica_election_results[0].topic.?);
    try testing.expectEqual(@as(usize, 2), resp.replica_election_results[0].partition_result.len);
    try testing.expectEqual(@as(i32, 0), resp.replica_election_results[0].partition_result[0].partition_id);
    try testing.expectEqual(@as(i16, 0), resp.replica_election_results[0].partition_result[0].error_code);
    try testing.expectEqual(@as(i32, 5), resp.replica_election_results[0].partition_result[1].partition_id);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.unknown_topic_or_partition)), resp.replica_election_results[0].partition_result[1].error_code);
}

test "Broker.handleRequest DescribeProducers returns only requested topic partitions" {
    const Req = generated.describe_producers_request.DescribeProducersRequest;
    const Resp = generated.describe_producers_response.DescribeProducersResponse;

    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();
    try testing.expect(broker.ensureTopic("producer-topic"));
    try testing.expect(broker.ensureTopic("other-topic"));

    try broker.producer_sequences.put(.{
        .producer_id = 61001,
        .partition_key = PartitionStore.hashPartitionKey("producer-topic", 0),
    }, .{
        .last_sequence = 7,
        .producer_epoch = 2,
    });
    try broker.producer_sequences.put(.{
        .producer_id = 61002,
        .partition_key = PartitionStore.hashPartitionKey("other-topic", 0),
    }, .{
        .last_sequence = 99,
        .producer_epoch = 1,
    });

    const producer_partitions = [_]i32{ 0, 2 };
    const missing_partitions = [_]i32{0};
    const topics = [_]Req.TopicRequest{
        .{ .name = "producer-topic", .partition_indexes = &producer_partitions },
        .{ .name = "missing-topic", .partition_indexes = &missing_partitions },
    };
    const req = Req{ .topics = &topics };

    var buf: [1024]u8 = undefined;
    var pos = buildTestRequest(&buf, 61, 0, 6100, header_mod.requestHeaderVersion(61, 0));
    req.serialize(&buf, &pos, 0);

    const response = broker.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var response_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(61, 0));
    defer response_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 6100), response_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer {
        for (resp.topics) |topic| {
            for (topic.partitions) |partition| {
                if (partition.active_producers.len > 0) testing.allocator.free(partition.active_producers);
            }
            if (topic.partitions.len > 0) testing.allocator.free(topic.partitions);
        }
        if (resp.topics.len > 0) testing.allocator.free(resp.topics);
    }

    try testing.expectEqual(@as(usize, 2), resp.topics.len);
    try testing.expectEqualStrings("producer-topic", resp.topics[0].name.?);
    try testing.expectEqual(@as(usize, 2), resp.topics[0].partitions.len);
    try testing.expectEqual(@as(i32, 0), resp.topics[0].partitions[0].partition_index);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.none)), resp.topics[0].partitions[0].error_code);
    try testing.expectEqual(@as(usize, 1), resp.topics[0].partitions[0].active_producers.len);
    try testing.expectEqual(@as(i64, 61001), resp.topics[0].partitions[0].active_producers[0].producer_id);
    try testing.expectEqual(@as(i32, 2), resp.topics[0].partitions[0].active_producers[0].producer_epoch);
    try testing.expectEqual(@as(i32, 7), resp.topics[0].partitions[0].active_producers[0].last_sequence);
    try testing.expectEqual(@as(i32, 2), resp.topics[0].partitions[1].partition_index);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.unknown_topic_or_partition)), resp.topics[0].partitions[1].error_code);
    try testing.expectEqual(@as(usize, 0), resp.topics[0].partitions[1].active_producers.len);
    try testing.expectEqualStrings("missing-topic", resp.topics[1].name.?);
    try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.unknown_topic_or_partition)), resp.topics[1].partitions[0].error_code);
}

test "Broker.handleRequest DescribeProducers rejects truncated request" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    var buf: [128]u8 = undefined;
    const req_len = buildTestRequest(&buf, 61, 0, 6101, header_mod.requestHeaderVersion(61, 0));
    try testing.expect(broker.handleRequest(buf[0..req_len]) == null);
}

// ---------------------------------------------------------------
// HIGH-priority gap tests: handler wire-protocol round-trips
// ---------------------------------------------------------------

test "Broker.handleRequest Fetch (key=1) returns produced data" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Step 1: Produce data via wire protocol (v0)
    {
        var buf: [512]u8 = undefined;
        var pos = buildTestRequest(&buf, 0, 0, 1, 1);
        ser.writeI16(&buf, &pos, 1); // acks
        ser.writeI32(&buf, &pos, 30000); // timeout_ms
        ser.writeI32(&buf, &pos, 1); // 1 topic
        ser.writeString(&buf, &pos, "fetch-test");
        ser.writeI32(&buf, &pos, 1); // 1 partition
        ser.writeI32(&buf, &pos, 0); // partition 0
        const records = "test-record-data";
        ser.writeI32(&buf, &pos, @intCast(records.len));
        @memcpy(buf[pos .. pos + records.len], records);
        pos += records.len;
        const resp = broker.handleRequest(buf[0..pos]);
        try testing.expect(resp != null);
        testing.allocator.free(resp.?);
    }

    // Step 2: Fetch via wire protocol (v0)
    {
        var buf: [512]u8 = undefined;
        var pos = buildTestRequest(&buf, 1, 0, 2, 1);
        ser.writeI32(&buf, &pos, -1); // replica_id
        ser.writeI32(&buf, &pos, 500); // max_wait_ms
        ser.writeI32(&buf, &pos, 1); // min_bytes
        ser.writeI32(&buf, &pos, 1); // 1 topic
        ser.writeString(&buf, &pos, "fetch-test");
        ser.writeI32(&buf, &pos, 1); // 1 partition
        ser.writeI32(&buf, &pos, 0); // partition 0
        ser.writeI64(&buf, &pos, 0); // fetch_offset
        ser.writeI32(&buf, &pos, 1048576); // partition_max_bytes

        const resp = broker.handleRequest(buf[0..pos]);
        try testing.expect(resp != null);
        defer testing.allocator.free(resp.?);

        // Parse response: correlation_id
        var rpos: usize = 0;
        const corr_id = ser.readI32(resp.?, &rpos);
        try testing.expectEqual(@as(i32, 2), corr_id);
    }
}

test "Broker.handleRequest ListOffsets (key=2) returns offsets" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Produce to create topic/partition state
    _ = broker.store.produce("lo-test", 0, "data") catch {};

    // ListOffsets v1
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 2, 1, 10, 1);
    ser.writeI32(&buf, &pos, -1); // replica_id
    // topics array: 1 topic
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, "lo-test");
    // partitions array: 1 partition
    ser.writeI32(&buf, &pos, 1);
    ser.writeI32(&buf, &pos, 0); // partition_index
    ser.writeI64(&buf, &pos, -1); // timestamp = -1 (latest)

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 10), corr_id);
}

test "Broker.handleRequest JoinGroup (key=11) returns member_id" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // JoinGroup v0
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 11, 0, 20, 1);
    ser.writeString(&buf, &pos, "test-group"); // group_id
    ser.writeI32(&buf, &pos, 30000); // session_timeout
    ser.writeString(&buf, &pos, ""); // member_id (empty = join as new)
    ser.writeString(&buf, &pos, "consumer"); // protocol_type
    // protocols array: 1 protocol
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, "range"); // protocol name
    ser.writeI32(&buf, &pos, 0); // protocol metadata (empty bytes)

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 20), corr_id);

    // Parse JoinGroup response: error_code + generation_id
    const error_code = ser.readI16(resp.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);
    const generation_id = ser.readI32(resp.?, &rpos);
    try testing.expect(generation_id >= 1);
}

test "Broker.handleRequest SyncGroup (key=14) after JoinGroup" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // First: JoinGroup to get a member_id
    const join_result = try broker.groups.joinGroup("sg-group", null, "consumer", null);

    // SyncGroup v0 — leader sends assignments
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 14, 0, 30, 1);
    ser.writeString(&buf, &pos, "sg-group"); // group_id
    ser.writeI32(&buf, &pos, join_result.generation_id); // generation_id
    ser.writeString(&buf, &pos, join_result.member_id); // member_id
    // assignments array: 1 member
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, join_result.member_id); // member
    const assign_data = "assign-data";
    ser.writeI32(&buf, &pos, @intCast(assign_data.len));
    @memcpy(buf[pos .. pos + assign_data.len], assign_data);
    pos += assign_data.len;

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 30), corr_id);

    // After SyncGroup, group should be STABLE
    const group = broker.groups.groups.getPtr("sg-group").?;
    try testing.expectEqual(@import("group_coordinator.zig").ConsumerGroup.GroupState.stable, group.state);
}

test "Broker.handleRequest OffsetCommit and OffsetFetch round-trip" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // OffsetCommit (key=8, v0)
    {
        var buf: [512]u8 = undefined;
        var pos = buildTestRequest(&buf, 8, 0, 40, 1);
        ser.writeString(&buf, &pos, "oc-group"); // group_id
        ser.writeI32(&buf, &pos, 1); // generation_id
        ser.writeString(&buf, &pos, "member-1"); // member_id
        // topics array: 1 topic
        ser.writeI32(&buf, &pos, 1);
        ser.writeString(&buf, &pos, "oc-topic");
        // partitions array: 1 partition
        ser.writeI32(&buf, &pos, 1);
        ser.writeI32(&buf, &pos, 0); // partition_index
        ser.writeI64(&buf, &pos, 42); // committed offset

        const resp = broker.handleRequest(buf[0..pos]);
        try testing.expect(resp != null);
        testing.allocator.free(resp.?);
    }

    // OffsetFetch (key=9, v1)
    {
        var buf: [512]u8 = undefined;
        var pos = buildTestRequest(&buf, 9, 1, 41, 1);
        ser.writeString(&buf, &pos, "oc-group"); // group_id
        // topics array: 1 topic
        ser.writeI32(&buf, &pos, 1);
        ser.writeString(&buf, &pos, "oc-topic");
        // partitions array: 1 partition
        ser.writeI32(&buf, &pos, 1);
        ser.writeI32(&buf, &pos, 0); // partition_index

        const resp = broker.handleRequest(buf[0..pos]);
        try testing.expect(resp != null);
        defer testing.allocator.free(resp.?);

        var rpos: usize = 0;
        const corr_id = ser.readI32(resp.?, &rpos);
        try testing.expectEqual(@as(i32, 41), corr_id);
    }

    // Verify offset was persisted in group coordinator
    const committed = try broker.groups.fetchOffset("oc-group", "oc-topic", 0);
    try testing.expectEqual(@as(i64, 42), committed.?);
}

test "Broker.handleRequest CreateTopics (key=19) creates topic" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // CreateTopics v0
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 19, 0, 50, 1);
    // topics array: 1 topic
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, "new-ct-topic"); // topic name
    ser.writeI32(&buf, &pos, 3); // num_partitions
    ser.writeI16(&buf, &pos, 1); // replication_factor
    ser.writeI32(&buf, &pos, 0); // assignments array (empty)
    ser.writeI32(&buf, &pos, 0); // configs array (empty)
    ser.writeI32(&buf, &pos, 30000); // timeout_ms

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 50), corr_id);

    // Verify topic exists in broker
    try testing.expect(broker.topics.contains("new-ct-topic"));
    const info = broker.topics.get("new-ct-topic").?;
    try testing.expectEqual(@as(i32, 3), info.num_partitions);
}

test "Broker.handleRequest AddPartitionsToTxn (key=24) after InitProducerId" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // InitProducerId first
    const init_result = try broker.txn_coordinator.initProducerId("txn-test");
    const pid = init_result.producer_id;

    // AddPartitionsToTxn v0
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 24, 0, 60, 1);
    ser.writeString(&buf, &pos, "txn-test"); // transactional_id
    ser.writeI64(&buf, &pos, pid); // producer_id
    ser.writeI16(&buf, &pos, init_result.producer_epoch); // producer_epoch
    // topics array: 1 topic
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, "txn-topic");
    // partitions array: 1 partition
    ser.writeI32(&buf, &pos, 1);
    ser.writeI32(&buf, &pos, 0);

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 60), corr_id);
}

test "Broker.handleRequest EndTxn (key=26) full transaction lifecycle" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // InitProducerId
    const init_result = try broker.txn_coordinator.initProducerId("txn-end");
    const pid = init_result.producer_id;

    // Add partition to transaction
    _ = try broker.txn_coordinator.addPartitionsToTxn(pid, init_result.producer_epoch, "txn-topic", 0);

    // EndTxn v0 (commit)
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 26, 0, 70, 1);
    ser.writeString(&buf, &pos, "txn-end"); // transactional_id
    ser.writeI64(&buf, &pos, pid); // producer_id
    ser.writeI16(&buf, &pos, init_result.producer_epoch); // producer_epoch
    ser.writeI8(&buf, &pos, 1); // committed = true

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 70), corr_id);
}

test "Broker setRaftState wires raft state" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Initially, raft_state should be null
    try testing.expect(broker.raft_state == null);

    var raft = RaftState.init(testing.allocator, 1, "test-cluster");
    defer raft.deinit();

    broker.setRaftState(&raft);
    try testing.expect(broker.raft_state != null);
    try testing.expectEqual(@as(i32, 1), broker.raft_state.?.node_id);
}

test "Broker flushPendingWal returns true in memory mode" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // In-memory broker has no S3 WAL batcher, so flushPendingWal should
    // return true immediately (the no-op / early-return path).
    const result = broker.flushPendingWal();
    try testing.expect(result);
}

test "Broker tick runs without crash in memory mode" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // tick() evicts expired group members and checks rebalance timeouts.
    // In-memory mode with no members, it should complete without error.
    broker.tick();
    broker.tick();
    broker.tick();
}

test "Broker fenced by controller rejects produce" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Fence the broker
    broker.is_fenced_by_controller = true;

    // Build a Produce v0 request
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 0, 0, 100, 1);
    ser.writeI16(&buf, &pos, 1); // acks = 1
    ser.writeI32(&buf, &pos, 30000); // timeout_ms
    ser.writeI32(&buf, &pos, 1); // topics array: 1 topic
    ser.writeString(&buf, &pos, "fenced-topic");
    ser.writeI32(&buf, &pos, 1); // partitions array: 1 partition
    ser.writeI32(&buf, &pos, 0); // partition_index
    const fake_records = "fake-record-batch-data";
    ser.writeI32(&buf, &pos, @intCast(fake_records.len));
    @memcpy(buf[pos .. pos + fake_records.len], fake_records);
    pos += fake_records.len;

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 100), corr_id);

    // handleNotController returns error code 41 (NOT_CONTROLLER) which tells
    // the client to refresh metadata and find the correct leader.
    const error_code = ser.readI16(resp.?, &rpos);
    try testing.expectEqual(@as(i16, 41), error_code);
}

test "Broker handleRequest DeleteTopics (key=20)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // First create a topic via ensureTopic so we have something to delete
    try testing.expect(broker.ensureTopic("del-topic"));
    try testing.expect(broker.topics.contains("del-topic"));

    // DeleteTopics v0
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 20, 0, 200, 1);
    // topics array: 1 topic
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, "del-topic");
    ser.writeI32(&buf, &pos, 30000); // timeout_ms

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 200), corr_id);

    // Topic should be removed after delete
    try testing.expect(!broker.topics.contains("del-topic"));
}

test "Broker handleRequest DescribeConfigs (key=32)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Create a topic first so DescribeConfigs has something to describe
    _ = broker.ensureTopic("cfg-topic");

    // DescribeConfigs v0
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 32, 0, 210, 1);
    // resources array: 1 resource
    ser.writeI32(&buf, &pos, 1);
    ser.writeI8(&buf, &pos, 2); // resource_type = TOPIC
    ser.writeString(&buf, &pos, "cfg-topic"); // resource_name
    ser.writeI32(&buf, &pos, 0); // config_keys array (empty = all configs)

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 210), corr_id);
}

test "Broker handleRequest Heartbeat (key=12)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Heartbeat v0: group_id + generation_id + member_id
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 12, 0, 220, 1);
    ser.writeString(&buf, &pos, "hb-group"); // group_id
    ser.writeI32(&buf, &pos, 1); // generation_id
    ser.writeString(&buf, &pos, "member-1"); // member_id

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(resp.?, &rpos);
    try testing.expectEqual(@as(i32, 220), corr_id);

    // Heartbeat for unknown member should return an error code,
    // but the response should still be well-formed.
    const error_code = ser.readI16(resp.?, &rpos);
    // Unknown member in unknown group — expect non-zero error
    try testing.expect(error_code != 0);
}

test "Broker kafka_server_api_errors_total is registered" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    try testing.expect(broker.metrics.labeled_counter_meta.contains("kafka_server_api_errors_total"));
}

test "Broker kafka_consumer_lag is registered" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    try testing.expect(broker.metrics.labeled_gauge_meta.contains("kafka_consumer_lag"));
}

test "Broker kafka_network_connections_active is registered" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    try testing.expect(broker.metrics.gauges.contains("kafka_network_connections_active"));
}

test "Broker error counter increments on API error" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Send a Heartbeat for an unknown group — this will return error code 16
    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 12, 0, 300, 1);
    ser.writeString(&buf, &pos, "nonexistent-group");
    ser.writeI32(&buf, &pos, 1);
    ser.writeString(&buf, &pos, "unknown-member");

    const resp = broker.handleRequest(buf[0..pos]);
    try testing.expect(resp != null);
    defer testing.allocator.free(resp.?);

    // Verify the error counter was incremented
    // Heartbeat for unknown group returns error 16 (COORDINATOR_NOT_AVAILABLE)
    const key = "kafka_server_api_errors_total{heartbeat,16}";
    const entry = broker.metrics.labeled_counters.get(key);
    try testing.expect(entry != null);
    try testing.expect(entry.?.value >= 1);
}

test "Broker consumer lag computed on offset commit" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Produce records to create a partition with next_offset > 0
    _ = broker.ensureTopic("lag-topic");

    // Directly commit offset with lag via the coordinator
    try broker.groups.commitOffsetWithLag("test-group", "lag-topic", 0, 5, 10);

    // Verify the lag metric was set (10 - 5 = 5)
    const key = "kafka_consumer_lag{test-group,lag-topic,0}";
    const entry = broker.metrics.labeled_gauges.get(key);
    try testing.expect(entry != null);
    try testing.expectEqual(@as(f64, 5.0), entry.?.value);
}

test "Broker extractResponseErrorCode finds error in Heartbeat response" {
    // Heartbeat v0 response: correlation_id(4) + error_code(2)
    // Response header v0 (non-flexible): no tagged fields
    var resp: [6]u8 = undefined;
    std.mem.writeInt(u32, resp[0..4], 42, .big); // correlation_id
    std.mem.writeInt(u16, resp[4..6], @bitCast(@as(i16, 16)), .big); // error_code = 16
    const ec = Broker.extractResponseErrorCode(&resp, 12, 0, 0);
    try testing.expectEqual(@as(i16, 16), ec);
}

test "Broker extractResponseErrorCode returns 0 for Produce" {
    // Produce has per-partition error codes, not a top-level one
    var resp: [10]u8 = undefined;
    @memset(&resp, 0);
    const ec = Broker.extractResponseErrorCode(&resp, 0, 0, 0);
    try testing.expectEqual(@as(i16, 0), ec);
}

test "Broker json_logger is initialized" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Verify json_logger can be used without crashing
    broker.json_logger.log(.info, "test log", 42);
}

test "Broker is_shutting_down rejects Produce with NOT_LEADER_OR_FOLLOWER" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    broker.is_shutting_down = true;

    // Build a Produce v0 request
    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 0, 0, 99, 1);

    const response = broker.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Parse correlation ID
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 99), corr_id);

    // Parse error code — should be NOT_LEADER_OR_FOLLOWER (6)
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 6), error_code);
}

test "Broker is_shutting_down allows ApiVersions (key 18)" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    broker.is_shutting_down = true;

    // Build an ApiVersions v0 request (non-flexible, header v1)
    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 0, 77, 1);

    const response = broker.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Parse correlation ID — should match
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 77), corr_id);

    // Parse error code — should be 0 (NONE), not 6
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);
}

test "Broker is_shutting_down rejects Fetch with NOT_LEADER_OR_FOLLOWER" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    broker.is_shutting_down = true;

    // Build a Fetch v0 request (api_key=1)
    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 1, 0, 55, 1);

    const response = broker.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 55), corr_id);

    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 6), error_code);
}

test "Broker is_shutting_down defaults to false" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    try testing.expect(!broker.is_shutting_down);
}

test "Broker shutdownFlushWal returns true when no S3 batcher" {
    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Without S3 storage configured, shutdownFlushWal is a no-op
    const result = broker.shutdownFlushWal();
    try testing.expect(result);
}

test "Broker tick WAL cleanup skips when no fs_wal" {
    const wal_mod = storage.wal;
    const S3WalBatcher = wal_mod.S3WalBatcher;

    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Wire up an S3WalBatcher with last_flushed_segment_id > 0
    // but no fs_wal (the common in-memory test configuration).
    // tick() should not crash — it checks for fs_wal before calling cleanupSegments.
    broker.store.s3_wal_batcher = S3WalBatcher.init(testing.allocator);
    broker.store.s3_wal_batcher.?.last_flushed_segment_id = 5;
    defer {
        broker.store.s3_wal_batcher.?.deinit();
        broker.store.s3_wal_batcher = null;
    }
    // fs_wal is null by default in memory mode — verify that
    try testing.expect(broker.store.fs_wal == null);

    broker.tick();
    broker.tick();
}

test "Broker tick WAL cleanup removes flushed segments" {
    const wal_mod = storage.wal;
    const Wal = wal_mod.Wal;
    const S3WalBatcher = wal_mod.S3WalBatcher;
    const fs = @import("fs_compat");

    const tmp_dir = "/tmp/zmq-broker-wal-cleanup-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    var broker = Broker.init(testing.allocator, 1, 9092);
    defer broker.deinit();

    // Set up a real filesystem WAL with tiny segments (each record triggers rollover)
    const small_segment: usize = 20;
    broker.store.fs_wal = Wal.init(testing.allocator, tmp_dir, small_segment);
    defer {
        if (broker.store.fs_wal) |*wal| wal.deinit();
        broker.store.fs_wal = null;
    }
    try broker.store.fs_wal.?.open();

    // Write 4 records to create multiple closed segments
    _ = try broker.store.fs_wal.?.append("aaaaaaaaaa"); // seg 0
    _ = try broker.store.fs_wal.?.append("bbbbbbbbbb"); // rolls seg 0, writes seg 1
    _ = try broker.store.fs_wal.?.append("cccccccccc"); // rolls seg 1, writes seg 2
    _ = try broker.store.fs_wal.?.append("dddddddddd"); // rolls seg 2, writes seg 3

    const segments_before = broker.store.fs_wal.?.segmentCount();
    try testing.expect(segments_before >= 4);

    // Set up S3WalBatcher with last_flushed_segment_id = 2
    // This means segments with ID <= 1 are safe to clean up.
    broker.store.s3_wal_batcher = S3WalBatcher.init(testing.allocator);
    broker.store.s3_wal_batcher.?.last_flushed_segment_id = 2;
    defer {
        broker.store.s3_wal_batcher.?.deinit();
        broker.store.s3_wal_batcher = null;
    }

    // tick() should clean up WAL segments with ID <= 1
    broker.tick();

    const segments_after = broker.store.fs_wal.?.segmentCount();
    try testing.expect(segments_after < segments_before);
}

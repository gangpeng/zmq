const std = @import("std");
const testing = std.testing;

/// Kafka API keys and AutoMQ protocol extensions.
///
/// Values are aligned with the JSON schemas in src/protocol/schemas.
/// Each API key has a set of valid versions and flexible versions.
pub const ApiKey = enum(i16) {
    // ---------------------------------------------------------------
    // Standard Kafka APIs
    // ---------------------------------------------------------------
    produce = 0,
    fetch = 1,
    list_offsets = 2,
    metadata = 3,
    leader_and_isr = 4,
    stop_replica = 5,
    update_metadata = 6,
    controlled_shutdown = 7,
    offset_commit = 8,
    offset_fetch = 9,
    find_coordinator = 10,
    join_group = 11,
    heartbeat = 12,
    leave_group = 13,
    sync_group = 14,
    describe_groups = 15,
    list_groups = 16,
    sasl_handshake = 17,
    api_versions = 18,
    create_topics = 19,
    delete_topics = 20,
    delete_records = 21,
    init_producer_id = 22,
    offset_for_leader_epoch = 23,
    add_partitions_to_txn = 24,
    add_offsets_to_txn = 25,
    end_txn = 26,
    write_txn_markers = 27,
    txn_offset_commit = 28,
    describe_acls = 29,
    create_acls = 30,
    delete_acls = 31,
    describe_configs = 32,
    alter_configs = 33,
    alter_replica_log_dirs = 34,
    describe_log_dirs = 35,
    sasl_authenticate = 36,
    create_partitions = 37,
    create_delegation_token = 38,
    renew_delegation_token = 39,
    expire_delegation_token = 40,
    describe_delegation_token = 41,
    delete_groups = 42,
    elect_leaders = 43,
    incremental_alter_configs = 44,
    alter_partition_reassignments = 45,
    list_partition_reassignments = 46,
    offset_delete = 47,
    describe_client_quotas = 48,
    alter_client_quotas = 49,
    describe_user_scram_credentials = 50,
    alter_user_scram_credentials = 51,
    vote = 52,
    begin_quorum_epoch = 53,
    end_quorum_epoch = 54,
    describe_quorum = 55,
    alter_partition = 56,
    update_features = 57,
    envelope = 58,
    fetch_snapshot = 59,
    describe_cluster = 60,
    describe_producers = 61,
    broker_registration = 62,
    broker_heartbeat = 63,
    unregister_broker = 64,
    describe_transactions = 65,
    list_transactions = 66,
    allocate_producer_ids = 67,
    consumer_group_heartbeat = 68,
    consumer_group_describe = 69,
    controller_registration = 70,
    get_telemetry_subscriptions = 71,
    push_telemetry = 72,
    assign_replicas_to_dirs = 73,
    list_client_metrics_resources = 74,
    describe_topic_partitions = 75,
    share_group_heartbeat = 76,
    share_group_describe = 77,
    share_fetch = 78,
    share_acknowledge = 79,
    add_raft_voter = 80,
    remove_raft_voter = 81,
    update_raft_voter = 82,
    initialize_share_group_state = 83,
    read_share_group_state = 84,
    write_share_group_state = 85,
    delete_share_group_state = 86,
    read_share_group_state_summary = 87,

    // ---------------------------------------------------------------
    // AutoMQ Extension APIs
    // ---------------------------------------------------------------
    create_streams = 501,
    open_streams = 502,
    close_streams = 503,
    delete_streams = 504,
    prepare_s3_object = 505,
    commit_stream_set_object = 506,
    commit_stream_object = 507,
    get_opening_streams = 508,
    get_kvs = 509,
    put_kvs = 510,
    delete_kvs = 511,
    trim_streams = 512,
    automq_register_node = 513,
    automq_get_nodes = 514,
    automq_zone_router = 515,
    automq_get_partition_snapshot = 516,
    update_license = 517,
    describe_license = 518,
    export_cluster_manifest = 519,
    get_next_node_id = 600,
    describe_streams = 601,
    automq_update_group = 602,

    _,

    /// Get a human-readable name for this API key.
    pub fn name(self: ApiKey) []const u8 {
        return switch (self) {
            .produce => "Produce",
            .fetch => "Fetch",
            .list_offsets => "ListOffsets",
            .metadata => "Metadata",
            .leader_and_isr => "LeaderAndIsr",
            .stop_replica => "StopReplica",
            .update_metadata => "UpdateMetadata",
            .controlled_shutdown => "ControlledShutdown",
            .offset_commit => "OffsetCommit",
            .offset_fetch => "OffsetFetch",
            .find_coordinator => "FindCoordinator",
            .join_group => "JoinGroup",
            .heartbeat => "Heartbeat",
            .leave_group => "LeaveGroup",
            .sync_group => "SyncGroup",
            .describe_groups => "DescribeGroups",
            .list_groups => "ListGroups",
            .sasl_handshake => "SaslHandshake",
            .api_versions => "ApiVersions",
            .create_topics => "CreateTopics",
            .delete_topics => "DeleteTopics",
            .delete_records => "DeleteRecords",
            .init_producer_id => "InitProducerId",
            .offset_for_leader_epoch => "OffsetForLeaderEpoch",
            .add_partitions_to_txn => "AddPartitionsToTxn",
            .add_offsets_to_txn => "AddOffsetsToTxn",
            .end_txn => "EndTxn",
            .write_txn_markers => "WriteTxnMarkers",
            .txn_offset_commit => "TxnOffsetCommit",
            .describe_acls => "DescribeAcls",
            .create_acls => "CreateAcls",
            .delete_acls => "DeleteAcls",
            .describe_configs => "DescribeConfigs",
            .alter_configs => "AlterConfigs",
            .alter_replica_log_dirs => "AlterReplicaLogDirs",
            .describe_log_dirs => "DescribeLogDirs",
            .sasl_authenticate => "SaslAuthenticate",
            .create_partitions => "CreatePartitions",
            .create_delegation_token => "CreateDelegationToken",
            .renew_delegation_token => "RenewDelegationToken",
            .expire_delegation_token => "ExpireDelegationToken",
            .describe_delegation_token => "DescribeDelegationToken",
            .delete_groups => "DeleteGroups",
            .elect_leaders => "ElectLeaders",
            .incremental_alter_configs => "IncrementalAlterConfigs",
            .alter_partition_reassignments => "AlterPartitionReassignments",
            .list_partition_reassignments => "ListPartitionReassignments",
            .offset_delete => "OffsetDelete",
            .describe_client_quotas => "DescribeClientQuotas",
            .alter_client_quotas => "AlterClientQuotas",
            .describe_user_scram_credentials => "DescribeUserScramCredentials",
            .alter_user_scram_credentials => "AlterUserScramCredentials",
            .vote => "Vote",
            .begin_quorum_epoch => "BeginQuorumEpoch",
            .end_quorum_epoch => "EndQuorumEpoch",
            .describe_quorum => "DescribeQuorum",
            .alter_partition => "AlterPartition",
            .update_features => "UpdateFeatures",
            .envelope => "Envelope",
            .fetch_snapshot => "FetchSnapshot",
            .describe_cluster => "DescribeCluster",
            .describe_producers => "DescribeProducers",
            .broker_registration => "BrokerRegistration",
            .broker_heartbeat => "BrokerHeartbeat",
            .unregister_broker => "UnregisterBroker",
            .describe_transactions => "DescribeTransactions",
            .list_transactions => "ListTransactions",
            .allocate_producer_ids => "AllocateProducerIds",
            .consumer_group_heartbeat => "ConsumerGroupHeartbeat",
            .consumer_group_describe => "ConsumerGroupDescribe",
            .controller_registration => "ControllerRegistration",
            .get_telemetry_subscriptions => "GetTelemetrySubscriptions",
            .push_telemetry => "PushTelemetry",
            .assign_replicas_to_dirs => "AssignReplicasToDirs",
            .list_client_metrics_resources => "ListClientMetricsResources",
            .describe_topic_partitions => "DescribeTopicPartitions",
            .share_group_heartbeat => "ShareGroupHeartbeat",
            .share_group_describe => "ShareGroupDescribe",
            .share_fetch => "ShareFetch",
            .share_acknowledge => "ShareAcknowledge",
            .add_raft_voter => "AddRaftVoter",
            .remove_raft_voter => "RemoveRaftVoter",
            .update_raft_voter => "UpdateRaftVoter",
            .initialize_share_group_state => "InitializeShareGroupState",
            .read_share_group_state => "ReadShareGroupState",
            .write_share_group_state => "WriteShareGroupState",
            .delete_share_group_state => "DeleteShareGroupState",
            .read_share_group_state_summary => "ReadShareGroupStateSummary",
            // AutoMQ extensions
            .create_streams => "CreateStreams",
            .open_streams => "OpenStreams",
            .close_streams => "CloseStreams",
            .delete_streams => "DeleteStreams",
            .prepare_s3_object => "PrepareS3Object",
            .commit_stream_set_object => "CommitStreamSetObject",
            .commit_stream_object => "CommitStreamObject",
            .get_opening_streams => "GetOpeningStreams",
            .get_kvs => "GetKvs",
            .put_kvs => "PutKvs",
            .delete_kvs => "DeleteKvs",
            .trim_streams => "TrimStreams",
            .automq_register_node => "AutomqRegisterNode",
            .automq_get_nodes => "AutomqGetNodes",
            .automq_zone_router => "AutomqZoneRouter",
            .automq_get_partition_snapshot => "AutomqGetPartitionSnapshot",
            .update_license => "UpdateLicense",
            .describe_license => "DescribeLicense",
            .export_cluster_manifest => "ExportClusterManifest",
            .get_next_node_id => "GetNextNodeId",
            .describe_streams => "DescribeStreams",
            .automq_update_group => "AutomqUpdateGroup",
            else => "Unknown",
        };
    }

    /// Whether this API key is an AutoMQ extension.
    pub fn isAutomqExtension(self: ApiKey) bool {
        return switch (self) {
            .create_streams,
            .open_streams,
            .close_streams,
            .delete_streams,
            .prepare_s3_object,
            .commit_stream_set_object,
            .commit_stream_object,
            .get_opening_streams,
            .get_kvs,
            .put_kvs,
            .delete_kvs,
            .trim_streams,
            .automq_register_node,
            .automq_get_nodes,
            .automq_zone_router,
            .automq_get_partition_snapshot,
            .update_license,
            .describe_license,
            .export_cluster_manifest,
            .get_next_node_id,
            .describe_streams,
            .automq_update_group,
            => true,
            else => false,
        };
    }

    /// Whether this API key is a KRaft/Raft protocol API.
    pub fn isRaftApi(self: ApiKey) bool {
        return switch (self) {
            .vote, .begin_quorum_epoch, .end_quorum_epoch, .describe_quorum, .add_raft_voter, .remove_raft_voter, .update_raft_voter => true,
            else => false,
        };
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "ApiKey name lookup" {
    try testing.expectEqualStrings("Produce", ApiKey.produce.name());
    try testing.expectEqualStrings("Fetch", ApiKey.fetch.name());
    try testing.expectEqualStrings("ApiVersions", ApiKey.api_versions.name());
    try testing.expectEqualStrings("CreateStreams", ApiKey.create_streams.name());
}

test "ApiKey from raw value" {
    const key: ApiKey = @enumFromInt(0);
    try testing.expectEqual(ApiKey.produce, key);

    const raft_key: ApiKey = @enumFromInt(52);
    try testing.expectEqual(ApiKey.vote, raft_key);

    try testing.expectEqual(ApiKey.get_telemetry_subscriptions, @as(ApiKey, @enumFromInt(71)));
    try testing.expectEqual(ApiKey.add_raft_voter, @as(ApiKey, @enumFromInt(80)));
    try testing.expectEqual(ApiKey.create_streams, @as(ApiKey, @enumFromInt(501)));
    try testing.expectEqual(ApiKey.get_next_node_id, @as(ApiKey, @enumFromInt(600)));
}

test "ApiKey isAutomqExtension" {
    try testing.expect(!ApiKey.produce.isAutomqExtension());
    try testing.expect(!ApiKey.fetch.isAutomqExtension());
    try testing.expect(ApiKey.create_streams.isAutomqExtension());
    try testing.expect(ApiKey.automq_zone_router.isAutomqExtension());
}

test "ApiKey isRaftApi" {
    try testing.expect(!ApiKey.produce.isRaftApi());
    try testing.expect(ApiKey.vote.isRaftApi());
    try testing.expect(ApiKey.begin_quorum_epoch.isRaftApi());
    try testing.expect(ApiKey.describe_quorum.isRaftApi());
}

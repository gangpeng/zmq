const std = @import("std");
const testing = std.testing;

/// Kafka API keys — every request type in the Kafka binary protocol.
///
/// Standard Kafka API keys (0-73) plus AutoMQ-compatible extensions (10001+).
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
    add_raft_voter = 71,
    remove_raft_voter = 72,
    update_raft_voter = 73,

    // ---------------------------------------------------------------
    // AutoMQ Extension APIs
    // ---------------------------------------------------------------
    // These use API keys 10001+ to avoid conflicts with upstream Kafka
    create_streams = 10001,
    delete_streams = 10002,
    open_streams = 10003,
    close_streams = 10004,
    trim_streams = 10005,
    describe_streams = 10006,
    prepare_s3_object = 10007,
    commit_stream_set_object = 10008,
    commit_stream_object = 10009,
    get_objects = 10010,
    get_opening_streams = 10011,
    get_kvs = 10012,
    put_kvs = 10013,
    delete_kvs = 10014,
    get_next_node_id = 10015,
    describe_cluster_ext = 10016,
    automq_register_node = 10017,
    automq_zone_router = 10018,

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
            .add_raft_voter => "AddRaftVoter",
            .remove_raft_voter => "RemoveRaftVoter",
            .update_raft_voter => "UpdateRaftVoter",
            // AutoMQ extensions
            .create_streams => "CreateStreams",
            .delete_streams => "DeleteStreams",
            .open_streams => "OpenStreams",
            .close_streams => "CloseStreams",
            .trim_streams => "TrimStreams",
            .describe_streams => "DescribeStreams",
            .prepare_s3_object => "PrepareS3Object",
            .commit_stream_set_object => "CommitStreamSetObject",
            .commit_stream_object => "CommitStreamObject",
            .get_objects => "GetObjects",
            .get_opening_streams => "GetOpeningStreams",
            .get_kvs => "GetKvs",
            .put_kvs => "PutKvs",
            .delete_kvs => "DeleteKvs",
            .get_next_node_id => "GetNextNodeId",
            .describe_cluster_ext => "DescribeClusterExt",
            .automq_register_node => "AutomqRegisterNode",
            .automq_zone_router => "AutomqZoneRouter",
            else => "Unknown",
        };
    }

    /// Whether this API key is an AutoMQ extension.
    pub fn isAutomqExtension(self: ApiKey) bool {
        return @intFromEnum(self) >= 10001;
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

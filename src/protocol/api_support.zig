const std = @import("std");
const testing = std.testing;

pub const VersionRange = struct {
    min: i16,
    max: i16,
};

pub const BrokerApiSupport = struct {
    key: i16,
    name: []const u8,
    metric: []const u8,
    min: i16,
    max: i16,

    pub fn range(self: BrokerApiSupport) VersionRange {
        return .{ .min = self.min, .max = self.max };
    }
};

pub const GeneratedRequestApi = struct {
    key: i16,
    name: []const u8,
    min: i16,
    max: i16,
};

pub const ControllerApiSupport = struct {
    key: i16,
    name: []const u8,
    min: i16,
    max: i16,

    pub fn range(self: ControllerApiSupport) VersionRange {
        return .{ .min = self.min, .max = self.max };
    }
};

/// APIs that the broker dispatches and advertises through ApiVersions.
///
/// Keep this table sorted by key. It is the single source of truth for
/// ApiVersions, version validation, and per-API metric names.
pub const broker_supported_apis = [_]BrokerApiSupport{
    .{ .key = 0, .name = "Produce", .metric = "kafka_server_produce_requests_total", .min = 0, .max = 11 },
    .{ .key = 1, .name = "Fetch", .metric = "kafka_server_fetch_requests_total", .min = 0, .max = 17 },
    .{ .key = 2, .name = "ListOffsets", .metric = "list_offsets", .min = 0, .max = 8 },
    .{ .key = 3, .name = "Metadata", .metric = "metadata", .min = 0, .max = 12 },
    .{ .key = 8, .name = "OffsetCommit", .metric = "offset_commit", .min = 0, .max = 9 },
    .{ .key = 9, .name = "OffsetFetch", .metric = "offset_fetch", .min = 0, .max = 9 },
    .{ .key = 10, .name = "FindCoordinator", .metric = "find_coordinator", .min = 0, .max = 6 },
    .{ .key = 11, .name = "JoinGroup", .metric = "join_group", .min = 0, .max = 9 },
    .{ .key = 12, .name = "Heartbeat", .metric = "heartbeat", .min = 0, .max = 4 },
    .{ .key = 13, .name = "LeaveGroup", .metric = "leave_group", .min = 0, .max = 5 },
    .{ .key = 14, .name = "SyncGroup", .metric = "sync_group", .min = 0, .max = 5 },
    .{ .key = 15, .name = "DescribeGroups", .metric = "describe_groups", .min = 0, .max = 5 },
    .{ .key = 16, .name = "ListGroups", .metric = "list_groups", .min = 0, .max = 5 },
    .{ .key = 17, .name = "SaslHandshake", .metric = "sasl_handshake", .min = 0, .max = 1 },
    .{ .key = 18, .name = "ApiVersions", .metric = "api_versions", .min = 0, .max = 4 },
    .{ .key = 19, .name = "CreateTopics", .metric = "create_topics", .min = 0, .max = 7 },
    .{ .key = 20, .name = "DeleteTopics", .metric = "delete_topics", .min = 0, .max = 6 },
    .{ .key = 21, .name = "DeleteRecords", .metric = "delete_records", .min = 0, .max = 2 },
    .{ .key = 22, .name = "InitProducerId", .metric = "init_producer_id", .min = 0, .max = 4 },
    .{ .key = 23, .name = "OffsetForLeaderEpoch", .metric = "offset_for_leader_epoch", .min = 0, .max = 4 },
    .{ .key = 24, .name = "AddPartitionsToTxn", .metric = "add_partitions_to_txn", .min = 0, .max = 4 },
    .{ .key = 25, .name = "AddOffsetsToTxn", .metric = "add_offsets_to_txn", .min = 0, .max = 3 },
    .{ .key = 26, .name = "EndTxn", .metric = "end_txn", .min = 0, .max = 3 },
    .{ .key = 27, .name = "WriteTxnMarkers", .metric = "write_txn_markers", .min = 0, .max = 1 },
    .{ .key = 28, .name = "TxnOffsetCommit", .metric = "txn_offset_commit", .min = 0, .max = 3 },
    .{ .key = 29, .name = "DescribeAcls", .metric = "describe_acls", .min = 0, .max = 3 },
    .{ .key = 30, .name = "CreateAcls", .metric = "create_acls", .min = 0, .max = 3 },
    .{ .key = 31, .name = "DeleteAcls", .metric = "delete_acls", .min = 0, .max = 3 },
    .{ .key = 32, .name = "DescribeConfigs", .metric = "describe_configs", .min = 0, .max = 4 },
    .{ .key = 33, .name = "AlterConfigs", .metric = "alter_configs", .min = 0, .max = 2 },
    .{ .key = 35, .name = "DescribeLogDirs", .metric = "describe_log_dirs", .min = 0, .max = 4 },
    .{ .key = 36, .name = "SaslAuthenticate", .metric = "sasl_authenticate", .min = 0, .max = 2 },
    .{ .key = 37, .name = "CreatePartitions", .metric = "create_partitions", .min = 0, .max = 3 },
    .{ .key = 42, .name = "DeleteGroups", .metric = "delete_groups", .min = 0, .max = 2 },
    .{ .key = 43, .name = "ElectLeaders", .metric = "elect_leaders", .min = 0, .max = 2 },
    .{ .key = 44, .name = "IncrementalAlterConfigs", .metric = "incremental_alter_configs", .min = 0, .max = 1 },
    .{ .key = 45, .name = "AlterPartitionReassignments", .metric = "alter_partition_reassignments", .min = 0, .max = 0 },
    .{ .key = 46, .name = "ListPartitionReassignments", .metric = "list_partition_reassignments", .min = 0, .max = 0 },
    .{ .key = 47, .name = "OffsetDelete", .metric = "offset_delete", .min = 0, .max = 0 },
    .{ .key = 48, .name = "DescribeClientQuotas", .metric = "describe_client_quotas", .min = 0, .max = 1 },
    .{ .key = 49, .name = "AlterClientQuotas", .metric = "alter_client_quotas", .min = 0, .max = 1 },
    .{ .key = 50, .name = "DescribeUserScramCredentials", .metric = "describe_user_scram_credentials", .min = 0, .max = 0 },
    .{ .key = 52, .name = "Vote", .metric = "vote", .min = 0, .max = 1 },
    .{ .key = 53, .name = "BeginQuorumEpoch", .metric = "begin_quorum_epoch", .min = 0, .max = 1 },
    .{ .key = 54, .name = "EndQuorumEpoch", .metric = "end_quorum_epoch", .min = 0, .max = 1 },
    .{ .key = 55, .name = "DescribeQuorum", .metric = "describe_quorum", .min = 0, .max = 2 },
    .{ .key = 60, .name = "DescribeCluster", .metric = "describe_cluster", .min = 0, .max = 1 },
    .{ .key = 61, .name = "DescribeProducers", .metric = "describe_producers", .min = 0, .max = 0 },
    .{ .key = 65, .name = "DescribeTransactions", .metric = "describe_transactions", .min = 0, .max = 0 },
    .{ .key = 66, .name = "ListTransactions", .metric = "list_transactions", .min = 0, .max = 1 },
    .{ .key = 75, .name = "DescribeTopicPartitions", .metric = "describe_topic_partitions", .min = 0, .max = 0 },
    .{ .key = 501, .name = "CreateStreams", .metric = "automq_create_streams", .min = 0, .max = 1 },
    .{ .key = 502, .name = "OpenStreams", .metric = "automq_open_streams", .min = 0, .max = 1 },
    .{ .key = 503, .name = "CloseStreams", .metric = "automq_close_streams", .min = 0, .max = 0 },
    .{ .key = 504, .name = "DeleteStreams", .metric = "automq_delete_streams", .min = 0, .max = 0 },
    .{ .key = 505, .name = "PrepareS3Object", .metric = "automq_prepare_s3_object", .min = 0, .max = 0 },
    .{ .key = 506, .name = "CommitStreamSetObject", .metric = "automq_commit_stream_set_object", .min = 0, .max = 1 },
    .{ .key = 507, .name = "CommitStreamObject", .metric = "automq_commit_stream_object", .min = 0, .max = 1 },
    .{ .key = 508, .name = "GetOpeningStreams", .metric = "automq_get_opening_streams", .min = 0, .max = 0 },
    .{ .key = 509, .name = "GetKVs", .metric = "automq_get_kvs", .min = 0, .max = 0 },
    .{ .key = 510, .name = "PutKVs", .metric = "automq_put_kvs", .min = 0, .max = 0 },
    .{ .key = 511, .name = "DeleteKVs", .metric = "automq_delete_kvs", .min = 0, .max = 0 },
    .{ .key = 512, .name = "TrimStreams", .metric = "automq_trim_streams", .min = 0, .max = 0 },
    .{ .key = 513, .name = "AutomqRegisterNode", .metric = "automq_register_node", .min = 0, .max = 0 },
    .{ .key = 514, .name = "AutomqGetNodes", .metric = "automq_get_nodes", .min = 0, .max = 0 },
    .{ .key = 515, .name = "AutomqZoneRouter", .metric = "automq_zone_router", .min = 0, .max = 1 },
    .{ .key = 516, .name = "AutomqGetPartitionSnapshot", .metric = "automq_get_partition_snapshot", .min = 0, .max = 2 },
    .{ .key = 517, .name = "UpdateLicense", .metric = "automq_update_license", .min = 0, .max = 0 },
    .{ .key = 518, .name = "DescribeLicense", .metric = "automq_describe_license", .min = 0, .max = 0 },
    .{ .key = 519, .name = "ExportClusterManifest", .metric = "automq_export_cluster_manifest", .min = 0, .max = 0 },
    .{ .key = 600, .name = "GetNextNodeId", .metric = "automq_get_next_node_id", .min = 0, .max = 0 },
    .{ .key = 601, .name = "DescribeStreams", .metric = "automq_describe_streams", .min = 0, .max = 0 },
    .{ .key = 602, .name = "AutomqUpdateGroup", .metric = "automq_update_group", .min = 0, .max = 0 },
};

/// APIs that the controller port dispatches and advertises through ApiVersions.
///
/// Keep this table sorted by key. Dynamic Raft voter APIs must use the
/// generated Kafka keys 80/81/82 rather than telemetry keys 71/72.
pub const controller_supported_apis = [_]ControllerApiSupport{
    .{ .key = 18, .name = "ApiVersions", .min = 0, .max = 4 },
    .{ .key = 52, .name = "Vote", .min = 0, .max = 1 },
    .{ .key = 53, .name = "BeginQuorumEpoch", .min = 0, .max = 1 },
    .{ .key = 54, .name = "EndQuorumEpoch", .min = 0, .max = 1 },
    .{ .key = 55, .name = "DescribeQuorum", .min = 0, .max = 2 },
    .{ .key = 62, .name = "BrokerRegistration", .min = 0, .max = 0 },
    .{ .key = 63, .name = "BrokerHeartbeat", .min = 0, .max = 0 },
    .{ .key = 67, .name = "AllocateProducerIds", .min = 0, .max = 0 },
    .{ .key = 80, .name = "AddRaftVoter", .min = 0, .max = 0 },
    .{ .key = 81, .name = "RemoveRaftVoter", .min = 0, .max = 0 },
};

/// Request schemas present in src/protocol/schemas. This is intentionally
/// broader than broker_supported_apis: generated structs alone do not mean the
/// broker implements semantic request handling.
pub const generated_request_apis = [_]GeneratedRequestApi{
    .{ .key = 0, .name = "ProduceRequest", .min = 0, .max = 11 },
    .{ .key = 1, .name = "FetchRequest", .min = 0, .max = 17 },
    .{ .key = 2, .name = "ListOffsetsRequest", .min = 0, .max = 9 },
    .{ .key = 3, .name = "MetadataRequest", .min = 0, .max = 12 },
    .{ .key = 4, .name = "LeaderAndIsrRequest", .min = 0, .max = 7 },
    .{ .key = 5, .name = "StopReplicaRequest", .min = 0, .max = 4 },
    .{ .key = 6, .name = "UpdateMetadataRequest", .min = 0, .max = 8 },
    .{ .key = 7, .name = "ControlledShutdownRequest", .min = 0, .max = 3 },
    .{ .key = 8, .name = "OffsetCommitRequest", .min = 0, .max = 9 },
    .{ .key = 9, .name = "OffsetFetchRequest", .min = 0, .max = 9 },
    .{ .key = 10, .name = "FindCoordinatorRequest", .min = 0, .max = 6 },
    .{ .key = 11, .name = "JoinGroupRequest", .min = 0, .max = 9 },
    .{ .key = 12, .name = "HeartbeatRequest", .min = 0, .max = 4 },
    .{ .key = 13, .name = "LeaveGroupRequest", .min = 0, .max = 5 },
    .{ .key = 14, .name = "SyncGroupRequest", .min = 0, .max = 5 },
    .{ .key = 15, .name = "DescribeGroupsRequest", .min = 0, .max = 5 },
    .{ .key = 16, .name = "ListGroupsRequest", .min = 0, .max = 5 },
    .{ .key = 17, .name = "SaslHandshakeRequest", .min = 0, .max = 1 },
    .{ .key = 18, .name = "ApiVersionsRequest", .min = 0, .max = 4 },
    .{ .key = 19, .name = "CreateTopicsRequest", .min = 0, .max = 7 },
    .{ .key = 20, .name = "DeleteTopicsRequest", .min = 0, .max = 6 },
    .{ .key = 21, .name = "DeleteRecordsRequest", .min = 0, .max = 2 },
    .{ .key = 22, .name = "InitProducerIdRequest", .min = 0, .max = 5 },
    .{ .key = 23, .name = "OffsetForLeaderEpochRequest", .min = 0, .max = 4 },
    .{ .key = 24, .name = "AddPartitionsToTxnRequest", .min = 0, .max = 5 },
    .{ .key = 25, .name = "AddOffsetsToTxnRequest", .min = 0, .max = 4 },
    .{ .key = 26, .name = "EndTxnRequest", .min = 0, .max = 4 },
    .{ .key = 27, .name = "WriteTxnMarkersRequest", .min = 0, .max = 1 },
    .{ .key = 28, .name = "TxnOffsetCommitRequest", .min = 0, .max = 4 },
    .{ .key = 29, .name = "DescribeAclsRequest", .min = 0, .max = 3 },
    .{ .key = 30, .name = "CreateAclsRequest", .min = 0, .max = 3 },
    .{ .key = 31, .name = "DeleteAclsRequest", .min = 0, .max = 3 },
    .{ .key = 32, .name = "DescribeConfigsRequest", .min = 0, .max = 4 },
    .{ .key = 33, .name = "AlterConfigsRequest", .min = 0, .max = 2 },
    .{ .key = 34, .name = "AlterReplicaLogDirsRequest", .min = 0, .max = 2 },
    .{ .key = 35, .name = "DescribeLogDirsRequest", .min = 0, .max = 4 },
    .{ .key = 36, .name = "SaslAuthenticateRequest", .min = 0, .max = 2 },
    .{ .key = 37, .name = "CreatePartitionsRequest", .min = 0, .max = 3 },
    .{ .key = 38, .name = "CreateDelegationTokenRequest", .min = 0, .max = 3 },
    .{ .key = 39, .name = "RenewDelegationTokenRequest", .min = 0, .max = 2 },
    .{ .key = 40, .name = "ExpireDelegationTokenRequest", .min = 0, .max = 2 },
    .{ .key = 41, .name = "DescribeDelegationTokenRequest", .min = 0, .max = 3 },
    .{ .key = 42, .name = "DeleteGroupsRequest", .min = 0, .max = 2 },
    .{ .key = 43, .name = "ElectLeadersRequest", .min = 0, .max = 2 },
    .{ .key = 44, .name = "IncrementalAlterConfigsRequest", .min = 0, .max = 1 },
    .{ .key = 45, .name = "AlterPartitionReassignmentsRequest", .min = 0, .max = 0 },
    .{ .key = 46, .name = "ListPartitionReassignmentsRequest", .min = 0, .max = 0 },
    .{ .key = 47, .name = "OffsetDeleteRequest", .min = 0, .max = 0 },
    .{ .key = 48, .name = "DescribeClientQuotasRequest", .min = 0, .max = 1 },
    .{ .key = 49, .name = "AlterClientQuotasRequest", .min = 0, .max = 1 },
    .{ .key = 50, .name = "DescribeUserScramCredentialsRequest", .min = 0, .max = 0 },
    .{ .key = 51, .name = "AlterUserScramCredentialsRequest", .min = 0, .max = 0 },
    .{ .key = 52, .name = "VoteRequest", .min = 0, .max = 1 },
    .{ .key = 53, .name = "BeginQuorumEpochRequest", .min = 0, .max = 1 },
    .{ .key = 54, .name = "EndQuorumEpochRequest", .min = 0, .max = 1 },
    .{ .key = 55, .name = "DescribeQuorumRequest", .min = 0, .max = 2 },
    .{ .key = 56, .name = "AlterPartitionRequest", .min = 0, .max = 3 },
    .{ .key = 57, .name = "UpdateFeaturesRequest", .min = 0, .max = 1 },
    .{ .key = 58, .name = "EnvelopeRequest", .min = 0, .max = 0 },
    .{ .key = 59, .name = "FetchSnapshotRequest", .min = 0, .max = 1 },
    .{ .key = 60, .name = "DescribeClusterRequest", .min = 0, .max = 1 },
    .{ .key = 61, .name = "DescribeProducersRequest", .min = 0, .max = 0 },
    .{ .key = 62, .name = "BrokerRegistrationRequest", .min = 0, .max = 4 },
    .{ .key = 63, .name = "BrokerHeartbeatRequest", .min = 0, .max = 1 },
    .{ .key = 64, .name = "UnregisterBrokerRequest", .min = 0, .max = 0 },
    .{ .key = 65, .name = "DescribeTransactionsRequest", .min = 0, .max = 0 },
    .{ .key = 66, .name = "ListTransactionsRequest", .min = 0, .max = 1 },
    .{ .key = 67, .name = "AllocateProducerIdsRequest", .min = 0, .max = 0 },
    .{ .key = 68, .name = "ConsumerGroupHeartbeatRequest", .min = 0, .max = 0 },
    .{ .key = 69, .name = "ConsumerGroupDescribeRequest", .min = 0, .max = 0 },
    .{ .key = 70, .name = "ControllerRegistrationRequest", .min = 0, .max = 0 },
    .{ .key = 71, .name = "GetTelemetrySubscriptionsRequest", .min = 0, .max = 0 },
    .{ .key = 72, .name = "PushTelemetryRequest", .min = 0, .max = 0 },
    .{ .key = 73, .name = "AssignReplicasToDirsRequest", .min = 0, .max = 0 },
    .{ .key = 74, .name = "ListClientMetricsResourcesRequest", .min = 0, .max = 0 },
    .{ .key = 75, .name = "DescribeTopicPartitionsRequest", .min = 0, .max = 0 },
    .{ .key = 76, .name = "ShareGroupHeartbeatRequest", .min = 0, .max = 0 },
    .{ .key = 77, .name = "ShareGroupDescribeRequest", .min = 0, .max = 0 },
    .{ .key = 78, .name = "ShareFetchRequest", .min = 0, .max = 0 },
    .{ .key = 79, .name = "ShareAcknowledgeRequest", .min = 0, .max = 0 },
    .{ .key = 80, .name = "AddRaftVoterRequest", .min = 0, .max = 0 },
    .{ .key = 81, .name = "RemoveRaftVoterRequest", .min = 0, .max = 0 },
    .{ .key = 82, .name = "UpdateRaftVoterRequest", .min = 0, .max = 0 },
    .{ .key = 83, .name = "InitializeShareGroupStateRequest", .min = 0, .max = 0 },
    .{ .key = 84, .name = "ReadShareGroupStateRequest", .min = 0, .max = 0 },
    .{ .key = 85, .name = "WriteShareGroupStateRequest", .min = 0, .max = 0 },
    .{ .key = 86, .name = "DeleteShareGroupStateRequest", .min = 0, .max = 0 },
    .{ .key = 87, .name = "ReadShareGroupStateSummaryRequest", .min = 0, .max = 0 },
    .{ .key = 501, .name = "CreateStreamsRequest", .min = 0, .max = 1 },
    .{ .key = 502, .name = "OpenStreamsRequest", .min = 0, .max = 1 },
    .{ .key = 503, .name = "CloseStreamsRequest", .min = 0, .max = 0 },
    .{ .key = 504, .name = "DeleteStreamsRequest", .min = 0, .max = 0 },
    .{ .key = 505, .name = "PrepareS3ObjectRequest", .min = 0, .max = 0 },
    .{ .key = 506, .name = "CommitStreamSetObjectRequest", .min = 0, .max = 1 },
    .{ .key = 507, .name = "CommitStreamObjectRequest", .min = 0, .max = 1 },
    .{ .key = 508, .name = "GetOpeningStreamsRequest", .min = 0, .max = 0 },
    .{ .key = 509, .name = "GetKVsRequest", .min = 0, .max = 0 },
    .{ .key = 510, .name = "PutKVsRequest", .min = 0, .max = 0 },
    .{ .key = 511, .name = "DeleteKVsRequest", .min = 0, .max = 0 },
    .{ .key = 512, .name = "TrimStreamsRequest", .min = 0, .max = 0 },
    .{ .key = 513, .name = "AutomqRegisterNodeRequest", .min = 0, .max = 0 },
    .{ .key = 514, .name = "AutomqGetNodesRequest", .min = 0, .max = 0 },
    .{ .key = 515, .name = "AutomqZoneRouterRequest", .min = 0, .max = 1 },
    .{ .key = 516, .name = "AutomqGetPartitionSnapshotRequest", .min = 0, .max = 2 },
    .{ .key = 517, .name = "UpdateLicenseRequest", .min = 0, .max = 0 },
    .{ .key = 518, .name = "DescribeLicenseRequest", .min = 0, .max = 0 },
    .{ .key = 519, .name = "ExportClusterManifestRequest", .min = 0, .max = 0 },
    .{ .key = 600, .name = "GetNextNodeIdRequest", .min = 0, .max = 0 },
    .{ .key = 601, .name = "DescribeStreamsRequest", .min = 0, .max = 0 },
    .{ .key = 602, .name = "AutomqUpdateGroupRequest", .min = 0, .max = 0 },
};

/// API keys with broker handler switch cases.
///
/// Version support still comes only from broker_supported_apis. This table makes
/// handler drift measurable: an API may not be advertised unless it has a switch
/// case, and non-advertised switch cases must be explicitly documented.
pub const broker_handler_api_keys = [_]i16{
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    35,
    36,
    37,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    52,
    53,
    54,
    55,
    60,
    61,
    65,
    66,
    75,
    501,
    502,
    503,
    504,
    505,
    506,
    507,
    508,
    509,
    510,
    511,
    512,
    513,
    514,
    515,
    516,
    517,
    518,
    519,
    600,
    601,
    602,
};

/// Handler cases that intentionally remain non-advertised until real
/// controller-backed semantics replace local/no-op legacy behavior.
pub const non_advertised_handler_api_keys = [_]i16{
    4, // LeaderAndIsr
    5, // StopReplica
    6, // UpdateMetadata
    7, // ControlledShutdown
};

/// Controller handler switch cases. Version support comes from
/// controller_supported_apis; this table only audits dispatch drift.
pub const controller_handler_api_keys = [_]i16{
    18,
    52,
    53,
    54,
    55,
    62,
    63,
    67,
    80,
    81,
};

pub fn findBrokerSupport(api_key: i16) ?BrokerApiSupport {
    for (broker_supported_apis) |api| {
        if (api.key == api_key) return api;
    }
    return null;
}

pub fn brokerVersionRange(api_key: i16) VersionRange {
    if (findBrokerSupport(api_key)) |api| return api.range();
    return .{ .min = -1, .max = -1 };
}

pub fn isBrokerVersionSupported(api_key: i16, api_version: i16) bool {
    const range = brokerVersionRange(api_key);
    return api_version >= range.min and api_version <= range.max;
}

pub fn brokerMetricName(api_key: i16) []const u8 {
    if (findBrokerSupport(api_key)) |api| return api.metric;
    return "";
}

pub fn brokerApiName(api_key: i16) []const u8 {
    if (findBrokerSupport(api_key)) |api| return api.name;
    return "Unknown";
}

pub fn findControllerSupport(api_key: i16) ?ControllerApiSupport {
    for (controller_supported_apis) |api| {
        if (api.key == api_key) return api;
    }
    return null;
}

pub fn controllerVersionRange(api_key: i16) VersionRange {
    if (findControllerSupport(api_key)) |api| return api.range();
    return .{ .min = -1, .max = -1 };
}

pub fn isControllerVersionSupported(api_key: i16, api_version: i16) bool {
    const range = controllerVersionRange(api_key);
    return api_version >= range.min and api_version <= range.max;
}

pub fn findGeneratedRequest(api_key: i16) ?GeneratedRequestApi {
    for (generated_request_apis) |api| {
        if (api.key == api_key) return api;
    }
    return null;
}

pub fn hasBrokerHandler(api_key: i16) bool {
    for (broker_handler_api_keys) |key| {
        if (key == api_key) return true;
    }
    return false;
}

pub fn hasControllerHandler(api_key: i16) bool {
    for (controller_handler_api_keys) |key| {
        if (key == api_key) return true;
    }
    return false;
}

pub fn isNonAdvertisedHandlerApi(api_key: i16) bool {
    for (non_advertised_handler_api_keys) |key| {
        if (key == api_key) return true;
    }
    return false;
}

pub fn isGeneratedAutoMqExtension(api_key: i16) bool {
    return findGeneratedRequest(api_key) != null and api_key >= 501;
}

fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    var count: usize = 0;
    var search_start: usize = 0;
    while (std.mem.indexOf(u8, haystack[search_start..], needle)) |rel_index| {
        count += 1;
        search_start += rel_index + needle.len;
    }
    return count;
}

fn switchBody(source: []const u8, start_marker: []const u8, end_marker: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, source, start_marker) orelse return null;
    const body_start = start + start_marker.len;
    const end_rel = std.mem.indexOf(u8, source[body_start..], end_marker) orelse return null;
    return source[body_start .. body_start + end_rel];
}

fn brokerHandleRequestSwitchBody(source: []const u8) ?[]const u8 {
    return switchBody(source, "const result = switch (api_key) {", "            else => self.handleUnsupported");
}

fn controllerHandleRequestSwitchBody(source: []const u8) ?[]const u8 {
    return switchBody(source, "return switch (api_key) {", "            else => self.handleUnsupported");
}

fn switchCaseKey(line: []const u8) ?i16 {
    if (std.mem.indexOf(u8, line, "=> self.handle") == null) return null;
    const arrow = std.mem.indexOf(u8, line, "=>") orelse return null;
    const lhs = std.mem.trim(u8, line[0..arrow], " \t");
    if (lhs.len == 0 or lhs[0] < '0' or lhs[0] > '9') return null;
    return std.fmt.parseInt(i16, lhs, 10) catch return null;
}

fn handleRequestSwitchHasCase(body: []const u8, key: i16) bool {
    var lines = std.mem.splitScalar(u8, body, '\n');
    while (lines.next()) |line| {
        if ((switchCaseKey(line) orelse continue) == key) return true;
    }
    return false;
}

test "broker API support table is sorted and unique" {
    var previous: ?i16 = null;
    for (broker_supported_apis) |api| {
        try testing.expect(api.min <= api.max);
        try testing.expect(api.metric.len > 0);
        if (previous) |prev| try testing.expect(api.key > prev);
        previous = api.key;
    }
}

test "controller API support table is sorted and unique" {
    var previous: ?i16 = null;
    for (controller_supported_apis) |api| {
        try testing.expect(api.min <= api.max);
        if (previous) |prev| try testing.expect(api.key > prev);
        previous = api.key;
    }
}

test "generated request API catalog is sorted and unique" {
    var previous: ?i16 = null;
    for (generated_request_apis) |api| {
        try testing.expect(api.min <= api.max);
        if (previous) |prev| try testing.expect(api.key > prev);
        previous = api.key;
    }
}

test "generated request API catalog matches generated index exports" {
    const generated_index_source = @embedFile("generated_index.zig");
    const request_exports = countOccurrences(generated_index_source, "_request = @import(\"generated/");
    const response_exports = countOccurrences(generated_index_source, "_response = @import(\"generated/");

    try testing.expectEqual(generated_request_apis.len, request_exports);
    try testing.expectEqual(generated_request_apis.len, response_exports);
}

test "broker handler API table is sorted and unique" {
    var previous: ?i16 = null;
    for (broker_handler_api_keys) |key| {
        if (previous) |prev| try testing.expect(key > prev);
        previous = key;
    }
}

test "controller handler API table is sorted and unique" {
    var previous: ?i16 = null;
    for (controller_handler_api_keys) |key| {
        if (previous) |prev| try testing.expect(key > prev);
        previous = key;
    }
}

test "broker handler switch matches audited handler API table" {
    const handler_source = @embedFile("../broker/handler.zig");
    const switch_body = brokerHandleRequestSwitchBody(handler_source) orelse return error.MissingBrokerHandlerSwitch;

    for (broker_handler_api_keys) |key| {
        try testing.expect(handleRequestSwitchHasCase(switch_body, key));
    }

    var lines = std.mem.splitScalar(u8, switch_body, '\n');
    while (lines.next()) |line| {
        const key = switchCaseKey(line) orelse continue;
        try testing.expect(hasBrokerHandler(key));
    }
}

test "controller handler switch matches audited handler API table" {
    const controller_source = @embedFile("../controller/controller.zig");
    const switch_body = controllerHandleRequestSwitchBody(controller_source) orelse return error.MissingControllerHandlerSwitch;

    for (controller_handler_api_keys) |key| {
        try testing.expect(handleRequestSwitchHasCase(switch_body, key));
    }

    var lines = std.mem.splitScalar(u8, switch_body, '\n');
    while (lines.next()) |line| {
        const key = switchCaseKey(line) orelse continue;
        try testing.expect(hasControllerHandler(key));
    }
}

test "broker supported APIs do not advertise versions beyond generated schemas" {
    for (broker_supported_apis) |api| {
        const schema = findGeneratedRequest(api.key) orelse return error.MissingGeneratedRequestSchema;
        try testing.expect(api.min >= schema.min);
        try testing.expect(api.max <= schema.max);
    }
}

test "controller supported APIs do not advertise versions beyond generated schemas" {
    for (controller_supported_apis) |api| {
        const schema = findGeneratedRequest(api.key) orelse return error.MissingGeneratedRequestSchema;
        try testing.expect(api.min >= schema.min);
        try testing.expect(api.max <= schema.max);
    }

    try testing.expect(findControllerSupport(71) == null);
    try testing.expect(findControllerSupport(72) == null);
    try testing.expectEqualStrings("AddRaftVoter", findControllerSupport(80).?.name);
    try testing.expectEqualStrings("RemoveRaftVoter", findControllerSupport(81).?.name);
}

test "broker supported APIs have handler switch coverage" {
    for (broker_supported_apis) |api| {
        try testing.expect(hasBrokerHandler(api.key));
    }
}

test "controller supported APIs have handler switch coverage" {
    for (controller_supported_apis) |api| {
        try testing.expect(hasControllerHandler(api.key));
    }
}

test "non-advertised handler cases are explicit legacy inter-broker RPCs" {
    for (broker_handler_api_keys) |key| {
        if (findBrokerSupport(key) != null) continue;

        try testing.expect(isNonAdvertisedHandlerApi(key));
        try testing.expect(findGeneratedRequest(key) != null);
        try testing.expect(!isBrokerVersionSupported(key, findGeneratedRequest(key).?.min));
    }

    for (non_advertised_handler_api_keys) |key| {
        try testing.expect(hasBrokerHandler(key));
        try testing.expect(findBrokerSupport(key) == null);
    }
}

test "broker support catalog does not advertise no-op inter-broker RPCs" {
    try testing.expect(findGeneratedRequest(4) != null);
    try testing.expect(findGeneratedRequest(5) != null);
    try testing.expect(findGeneratedRequest(6) != null);
    try testing.expect(findGeneratedRequest(7) != null);
    try testing.expect(findBrokerSupport(4) == null);
    try testing.expect(findBrokerSupport(5) == null);
    try testing.expect(findBrokerSupport(6) == null);
    try testing.expect(findBrokerSupport(7) == null);
}

test "canonical API support covers corrected Kafka and AutoMQ extension keys" {
    try testing.expectEqual(@as(i16, 2), findGeneratedRequest(42).?.max); // DeleteGroups
    try testing.expectEqualStrings("IncrementalAlterConfigs", findBrokerSupport(44).?.name);
    try testing.expectEqualStrings("AlterPartitionReassignments", findBrokerSupport(45).?.name);
    try testing.expectEqualStrings("ListPartitionReassignments", findBrokerSupport(46).?.name);
    try testing.expectEqualStrings("CreateStreamsRequest", findGeneratedRequest(501).?.name);
    try testing.expectEqualStrings("AutomqGetNodesRequest", findGeneratedRequest(514).?.name);
    try testing.expectEqualStrings("GetNextNodeIdRequest", findGeneratedRequest(600).?.name);
}

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
    .{ .key = 16, .name = "ListGroups", .metric = "list_groups", .min = 0, .max = 4 },
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
    .{ .key = 52, .name = "Vote", .metric = "vote", .min = 0, .max = 1 },
    .{ .key = 53, .name = "BeginQuorumEpoch", .metric = "begin_quorum_epoch", .min = 0, .max = 1 },
    .{ .key = 54, .name = "EndQuorumEpoch", .metric = "end_quorum_epoch", .min = 0, .max = 1 },
    .{ .key = 55, .name = "DescribeQuorum", .metric = "describe_quorum", .min = 0, .max = 2 },
    .{ .key = 60, .name = "DescribeCluster", .metric = "describe_cluster", .min = 0, .max = 1 },
    .{ .key = 61, .name = "DescribeProducers", .metric = "describe_producers", .min = 0, .max = 0 },
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

pub fn findGeneratedRequest(api_key: i16) ?GeneratedRequestApi {
    for (generated_request_apis) |api| {
        if (api.key == api_key) return api;
    }
    return null;
}

pub fn isGeneratedAutoMqExtension(api_key: i16) bool {
    return findGeneratedRequest(api_key) != null and api_key >= 501;
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

test "generated request API catalog is sorted and unique" {
    var previous: ?i16 = null;
    for (generated_request_apis) |api| {
        try testing.expect(api.min <= api.max);
        if (previous) |prev| try testing.expect(api.key > prev);
        previous = api.key;
    }
}

test "broker supported APIs do not advertise versions beyond generated schemas" {
    for (broker_supported_apis) |api| {
        const schema = findGeneratedRequest(api.key) orelse return error.MissingGeneratedRequestSchema;
        try testing.expect(api.min >= schema.min);
        try testing.expect(api.max <= schema.max);
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

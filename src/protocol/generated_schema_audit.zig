const std = @import("std");
const testing = std.testing;

const SourceExpectation = struct {
    label: []const u8,
    source: []const u8,
    needles: []const []const u8,
};

fn expectContains(label: []const u8, source: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, source, needle) == null) {
        std.debug.print("missing generated schema audit needle for {s}: {s}\n", .{ label, needle });
        return error.GeneratedSchemaAuditNeedleMissing;
    }
}

test "generated nullable array fields preserve null distinct from empty" {
    const expectations = [_]SourceExpectation{
        .{
            .label = "AlterPartitionReassignmentsRequest.Topics.Partitions.Replicas",
            .source = @embedFile("generated/alter_partition_reassignments_request.zig"),
            .needles = &.{
                "replicas: ?[]const i32 = null,",
                "if (self.replicas) |replicas|",
                "const replicas_len: ?usize",
                "result.replicas = null;",
            },
        },
        .{
            .label = "AutomqGetPartitionSnapshotResponse.Topics.Partitions.StreamMetadata",
            .source = @embedFile("generated/automq_get_partition_snapshot_response.zig"),
            .needles = &.{
                "stream_metadata: ?[]const StreamMetadata = null,",
                "if (self.stream_metadata) |stream_metadata|",
                "const stream_metadata_len: ?usize",
                "result.stream_metadata = null;",
            },
        },
        .{
            .label = "ConsumerGroupHeartbeatRequest.SubscribedTopicNames",
            .source = @embedFile("generated/consumer_group_heartbeat_request.zig"),
            .needles = &.{
                "subscribed_topic_names: ?[]const ?[]const u8 = null,",
                "if (self.subscribed_topic_names) |subscribed_topic_names|",
                "const subscribed_topic_names_len: ?usize",
                "result.subscribed_topic_names = null;",
            },
        },
        .{
            .label = "ConsumerGroupHeartbeatRequest.TopicPartitions",
            .source = @embedFile("generated/consumer_group_heartbeat_request.zig"),
            .needles = &.{
                "topic_partitions: ?[]const TopicPartitions = null,",
                "if (self.topic_partitions) |topic_partitions|",
                "const topic_partitions_len: ?usize",
                "result.topic_partitions = null;",
            },
        },
        .{
            .label = "CreatePartitionsRequest.Topics.Assignments",
            .source = @embedFile("generated/create_partitions_request.zig"),
            .needles = &.{
                "assignments: ?[]const CreatePartitionsAssignment = null,",
                "if (self.assignments) |assignments|",
                "const assignments_len: ?usize",
                "result.assignments = null;",
            },
        },
        .{
            .label = "CreateTopicsResponse.Topics.Configs",
            .source = @embedFile("generated/create_topics_response.zig"),
            .needles = &.{
                "configs: ?[]const CreatableTopicConfigs = null,",
                "if (self.configs) |configs|",
                "const configs_len: ?usize",
                "result.configs = null;",
            },
        },
        .{
            .label = "DescribeClientQuotasResponse.Entries",
            .source = @embedFile("generated/describe_client_quotas_response.zig"),
            .needles = &.{
                "entries: ?[]const EntryData = null,",
                "if (self.entries) |entries|",
                "const entries_len: ?usize",
                "result.entries = null;",
            },
        },
        .{
            .label = "DescribeConfigsRequest.Resources.ConfigurationKeys",
            .source = @embedFile("generated/describe_configs_request.zig"),
            .needles = &.{
                "configuration_keys: ?[]const ?[]const u8 = null,",
                "if (self.configuration_keys) |configuration_keys|",
                "const configuration_keys_len: ?usize",
                "result.configuration_keys = null;",
            },
        },
        .{
            .label = "DescribeDelegationTokenRequest.Owners",
            .source = @embedFile("generated/describe_delegation_token_request.zig"),
            .needles = &.{
                "owners: ?[]const DescribeDelegationTokenOwner = null,",
                "if (self.owners) |owners|",
                "const owners_len: ?usize",
                "result.owners = null;",
            },
        },
        .{
            .label = "DescribeLogDirsRequest.Topics",
            .source = @embedFile("generated/describe_log_dirs_request.zig"),
            .needles = &.{
                "topics: ?[]const DescribableLogDirTopic = null,",
                "if (self.topics) |topics|",
                "const topics_len: ?usize",
                "result.topics = null;",
            },
        },
        .{
            .label = "DescribeTopicPartitionsResponse.Topics.Partitions.EligibleLeaderReplicas",
            .source = @embedFile("generated/describe_topic_partitions_response.zig"),
            .needles = &.{
                "eligible_leader_replicas: ?[]const i32 = null,",
                "if (self.eligible_leader_replicas) |eligible_leader_replicas|",
                "const eligible_leader_replicas_len: ?usize",
                "result.eligible_leader_replicas = null;",
            },
        },
        .{
            .label = "DescribeTopicPartitionsResponse.Topics.Partitions.LastKnownElr",
            .source = @embedFile("generated/describe_topic_partitions_response.zig"),
            .needles = &.{
                "last_known_elr: ?[]const i32 = null,",
                "if (self.last_known_elr) |last_known_elr|",
                "const last_known_elr_len: ?usize",
                "result.last_known_elr = null;",
            },
        },
        .{
            .label = "DescribeUserScramCredentialsRequest.Users",
            .source = @embedFile("generated/describe_user_scram_credentials_request.zig"),
            .needles = &.{
                "users: ?[]const UserName = null,",
                "if (self.users) |users|",
                "const users_len: ?usize",
                "result.users = null;",
            },
        },
        .{
            .label = "ElectLeadersRequest.TopicPartitions",
            .source = @embedFile("generated/elect_leaders_request.zig"),
            .needles = &.{
                "topic_partitions: ?[]const TopicPartitions = null,",
                "if (self.topic_partitions) |topic_partitions|",
                "const topic_partitions_len: ?usize",
                "result.topic_partitions = null;",
            },
        },
        .{
            .label = "FetchResponse.Responses.Partitions.AbortedTransactions",
            .source = @embedFile("generated/fetch_response.zig"),
            .needles = &.{
                "aborted_transactions: ?[]const AbortedTransaction = null,",
                "if (self.aborted_transactions) |aborted_transactions|",
                "const aborted_transactions_len: ?usize",
                "result.aborted_transactions = null;",
            },
        },
        .{
            .label = "ListPartitionReassignmentsRequest.Topics",
            .source = @embedFile("generated/list_partition_reassignments_request.zig"),
            .needles = &.{
                "topics: ?[]const ListPartitionReassignmentsTopics = null,",
                "if (self.topics) |topics|",
                "const topics_len: ?usize",
                "result.topics = null;",
            },
        },
        .{
            .label = "MetadataRequest.Topics",
            .source = @embedFile("generated/metadata_request.zig"),
            .needles = &.{
                "topics: ?[]const MetadataRequestTopic = &.{},",
                "if (self.topics) |topics|",
                "const topics_len: ?usize",
                "result.topics = null;",
            },
        },
        .{
            .label = "OffsetFetchRequest.Topics",
            .source = @embedFile("generated/offset_fetch_request.zig"),
            .needles = &.{
                "topics: ?[]const OffsetFetchRequestTopic = &.{},",
                "if (self.topics) |topics|",
                "const topics_len: ?usize",
                "result.topics = null;",
            },
        },
        .{
            .label = "OffsetFetchRequest.Groups.Topics",
            .source = @embedFile("generated/offset_fetch_request.zig"),
            .needles = &.{
                "topics: ?[]const OffsetFetchRequestTopics = &.{},",
                "if (self.topics) |topics|",
                "const topics_len: ?usize",
                "result.topics = null;",
            },
        },
        .{
            .label = "ShareGroupHeartbeatRequest.SubscribedTopicNames",
            .source = @embedFile("generated/share_group_heartbeat_request.zig"),
            .needles = &.{
                "subscribed_topic_names: ?[]const ?[]const u8 = null,",
                "if (self.subscribed_topic_names) |subscribed_topic_names|",
                "const subscribed_topic_names_len: ?usize",
                "result.subscribed_topic_names = null;",
            },
        },
    };

    try testing.expectEqual(@as(usize, 20), expectations.len);
    for (expectations) |entry| {
        for (entry.needles) |needle| {
            try expectContains(entry.label, entry.source, needle);
        }
    }
}

test "generated tagged fields keep schema-visible encode decode coverage" {
    const expectations = [_]SourceExpectation{
        .{
            .label = "ApiVersionsResponse.SupportedFeatures tag 0",
            .source = @embedFile("generated/api_versions_response.zig"),
            .needles = &.{
                "supported_features: []const SupportedFeatureKey = &.{},",
                "writeTaggedFieldHeader(buf, pos, 0, supportedFeaturesTagSize",
                "var seen_supported_features = false;",
                "if (seen_supported_features) return error.DuplicateTaggedField;",
            },
        },
        .{
            .label = "ApiVersionsResponse.FinalizedFeaturesEpoch tag 1",
            .source = @embedFile("generated/api_versions_response.zig"),
            .needles = &.{
                "finalized_features_epoch: i64 = -1,",
                "writeTaggedFieldHeader(buf, pos, 1, 8);",
                "var seen_finalized_features_epoch = false;",
                "if (seen_finalized_features_epoch) return error.DuplicateTaggedField;",
            },
        },
        .{
            .label = "ApiVersionsResponse.FinalizedFeatures tag 2",
            .source = @embedFile("generated/api_versions_response.zig"),
            .needles = &.{
                "finalized_features: []const FinalizedFeatureKey = &.{},",
                "writeTaggedFieldHeader(buf, pos, 2, finalizedFeaturesTagSize",
                "var seen_finalized_features = false;",
                "if (seen_finalized_features) return error.DuplicateTaggedField;",
            },
        },
        .{
            .label = "ApiVersionsResponse.ZkMigrationReady tag 3",
            .source = @embedFile("generated/api_versions_response.zig"),
            .needles = &.{
                "zk_migration_ready: bool = false,",
                "writeTaggedFieldHeader(buf, pos, 3, 1);",
                "var seen_zk_migration_ready = false;",
                "if (seen_zk_migration_ready) return error.DuplicateTaggedField;",
            },
        },
        .{
            .label = "BeginQuorumEpochResponse.NodeEndpoints tag 0",
            .source = @embedFile("generated/begin_quorum_epoch_response.zig"),
            .needles = &.{
                "node_endpoints: []const NodeEndpoint = &.{},",
                "const has_node_endpoints = self.node_endpoints.len > 0;",
                "var seen_node_endpoints = false;",
                "result.node_endpoints = try readNodeEndpointsTag",
            },
        },
        .{
            .label = "BrokerHeartbeatRequest.OfflineLogDirs tag 0",
            .source = @embedFile("generated/broker_heartbeat_request.zig"),
            .needles = &.{
                "offline_log_dirs: []const [16]u8 = &.{},",
                "const has_offline_log_dirs = version >= 1 and self.offline_log_dirs.len > 0;",
                "var seen_offline_log_dirs = false;",
                "result.offline_log_dirs = try readOfflineLogDirsTag",
            },
        },
        .{
            .label = "CreateTopicsResponse.TopicConfigErrorCode tag 0",
            .source = @embedFile("generated/create_topics_response.zig"),
            .needles = &.{
                "topic_config_error_code: i16 = 0,",
                "const has_topic_config_error_code = version >= 5 and self.topic_config_error_code != 0;",
                "var seen_topic_config_error_code = false;",
                "result.topic_config_error_code = ser.readI16",
            },
        },
        .{
            .label = "EndQuorumEpochResponse.NodeEndpoints tag 0",
            .source = @embedFile("generated/end_quorum_epoch_response.zig"),
            .needles = &.{
                "node_endpoints: []const NodeEndpoint = &.{},",
                "const has_node_endpoints = self.node_endpoints.len > 0;",
                "var seen_node_endpoints = false;",
                "result.node_endpoints = try readNodeEndpointsTag",
            },
        },
        .{
            .label = "FetchRequest.ClusterId tag 0",
            .source = @embedFile("generated/fetch_request.zig"),
            .needles = &.{
                "cluster_id: ?[]const u8 = null,",
                "const has_cluster_id = version >= 12 and self.cluster_id != null;",
                "var seen_cluster_id = false;",
                "result.cluster_id = try ser.readCompactString",
            },
        },
        .{
            .label = "FetchRequest.ReplicaState tag 1",
            .source = @embedFile("generated/fetch_request.zig"),
            .needles = &.{
                "replica_state: ReplicaState = .{},",
                "const has_replica_state = version >= 15 and !self.replica_state.isDefault();",
                "var seen_replica_state = false;",
                "result.replica_state = try ReplicaState.deserialize",
            },
        },
        .{
            .label = "FetchRequest.ReplicaDirectoryId tag 0",
            .source = @embedFile("generated/fetch_request.zig"),
            .needles = &.{
                "replica_directory_id: [16]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },",
                "const has_replica_directory_id = version >= 17 and !isZeroUuid(self.replica_directory_id);",
                "var seen_replica_directory_id = false;",
                "result.replica_directory_id = try ser.readUuid",
            },
        },
        .{
            .label = "FetchResponse.DivergingEpoch tag 0",
            .source = @embedFile("generated/fetch_response.zig"),
            .needles = &.{
                "diverging_epoch: EpochEndOffset = .{},",
                "const has_diverging_epoch = !self.diverging_epoch.isDefault();",
                "var seen_diverging_epoch = false;",
                "result.diverging_epoch = try EpochEndOffset.deserialize",
            },
        },
        .{
            .label = "FetchResponse.CurrentLeader tag 1",
            .source = @embedFile("generated/fetch_response.zig"),
            .needles = &.{
                "current_leader: LeaderIdAndEpoch = .{},",
                "const has_current_leader = !self.current_leader.isDefault();",
                "var seen_current_leader = false;",
                "result.current_leader = try LeaderIdAndEpoch.deserialize",
            },
        },
        .{
            .label = "FetchResponse.SnapshotId tag 2",
            .source = @embedFile("generated/fetch_response.zig"),
            .needles = &.{
                "snapshot_id: SnapshotId = .{},",
                "const has_snapshot_id = !self.snapshot_id.isDefault();",
                "var seen_snapshot_id = false;",
                "result.snapshot_id = try SnapshotId.deserialize",
            },
        },
        .{
            .label = "FetchResponse.NodeEndpoints tag 0",
            .source = @embedFile("generated/fetch_response.zig"),
            .needles = &.{
                "node_endpoints: []const NodeEndpoint = &.{},",
                "const has_node_endpoints = version >= 16 and self.node_endpoints.len > 0;",
                "var seen_node_endpoints = false;",
                "result.node_endpoints = try readNodeEndpointsTag",
            },
        },
        .{
            .label = "FetchSnapshotRequest.ClusterId tag 0",
            .source = @embedFile("generated/fetch_snapshot_request.zig"),
            .needles = &.{
                "cluster_id: ?[]const u8 = null,",
                "const has_cluster_id = self.cluster_id != null;",
                "var seen_cluster_id = false;",
                "result.cluster_id = try ser.readCompactString",
            },
        },
        .{
            .label = "FetchSnapshotRequest.ReplicaDirectoryId tag 0",
            .source = @embedFile("generated/fetch_snapshot_request.zig"),
            .needles = &.{
                "replica_directory_id: [16]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },",
                "const has_replica_directory_id = version >= 1 and !isZeroUuid(self.replica_directory_id);",
                "var seen_replica_directory_id = false;",
                "result.replica_directory_id = try ser.readUuid",
            },
        },
        .{
            .label = "FetchSnapshotResponse.CurrentLeader tag 0",
            .source = @embedFile("generated/fetch_snapshot_response.zig"),
            .needles = &.{
                "current_leader: LeaderIdAndEpoch = .{},",
                "const has_current_leader = !self.current_leader.isDefault();",
                "var seen_current_leader = false;",
                "result.current_leader = try LeaderIdAndEpoch.deserialize",
            },
        },
        .{
            .label = "FetchSnapshotResponse.NodeEndpoints tag 0",
            .source = @embedFile("generated/fetch_snapshot_response.zig"),
            .needles = &.{
                "node_endpoints: []const NodeEndpoint = &.{},",
                "const has_node_endpoints = version >= 1 and self.node_endpoints.len > 0;",
                "var seen_node_endpoints = false;",
                "result.node_endpoints = try readNodeEndpointsTag",
            },
        },
        .{
            .label = "ProduceResponse.CurrentLeader tag 0",
            .source = @embedFile("generated/produce_response.zig"),
            .needles = &.{
                "current_leader: LeaderIdAndEpoch = .{},",
                "const has_current_leader = version >= 10 and !self.current_leader.isDefault();",
                "var seen_current_leader = false;",
                "result.current_leader = try LeaderIdAndEpoch.deserialize",
            },
        },
        .{
            .label = "ProduceResponse.NodeEndpoints tag 0",
            .source = @embedFile("generated/produce_response.zig"),
            .needles = &.{
                "node_endpoints: []const NodeEndpoint = &.{},",
                "const has_node_endpoints = version >= 10 and self.node_endpoints.len > 0;",
                "var seen_node_endpoints = false;",
                "result.node_endpoints = try readNodeEndpointsTag",
            },
        },
        .{
            .label = "UpdateMetadataRequest.Type tag 0",
            .source = @embedFile("generated/update_metadata_request.zig"),
            .needles = &.{
                "metadata_type: i8 = 0,",
                "const has_metadata_type = version >= 8 and self.metadata_type != 0;",
                "var seen_metadata_type = false;",
                "result.metadata_type = ser.readI8",
            },
        },
        .{
            .label = "VoteResponse.NodeEndpoints tag 0",
            .source = @embedFile("generated/vote_response.zig"),
            .needles = &.{
                "node_endpoints: []const NodeEndpoint = &.{},",
                "const has_node_endpoints = version >= 1 and self.node_endpoints.len > 0;",
                "var seen_node_endpoints = false;",
                "result.node_endpoints = try readNodeEndpointsTag",
            },
        },
    };

    try testing.expectEqual(@as(usize, 23), expectations.len);
    for (expectations) |entry| {
        for (entry.needles) |needle| {
            try expectContains(entry.label, entry.source, needle);
        }
    }
}

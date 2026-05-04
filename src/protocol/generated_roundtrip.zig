const std = @import("std");
const generated = @import("generated_index.zig");
const ser = @import("serialization.zig");

const testing = std.testing;
const default_round_trip_versions = [_]i16{
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
};

fn roundTripDefault(comptime Message: type, comptime version: i16) !void {
    var value = Message{};
    const size = value.calcSize(version);

    const buf = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(buf);

    var pos: usize = 0;
    value.serialize(buf, &pos, version);
    try testing.expectEqual(size, pos);

    var read_pos: usize = 0;
    const decoded = try Message.deserialize(testing.allocator, buf, &read_pos, version);
    try testing.expectEqual(pos, read_pos);
    try testing.expectEqual(size, decoded.calcSize(version));
}

fn roundTripDefaultVersions(comptime Message: type) !void {
    inline for (default_round_trip_versions) |version| {
        try roundTripDefault(Message, version);
    }
}

fn expectGoldenRoundTrip(comptime Message: type, value: Message, comptime version: i16, expected: []const u8) !void {
    const size = value.calcSize(version);
    try testing.expectEqual(expected.len, size);

    const buf = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(buf);

    var pos: usize = 0;
    value.serialize(buf, &pos, version);
    try testing.expectEqual(expected.len, pos);
    try testing.expectEqualSlices(u8, expected, buf);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var read_pos: usize = 0;
    const decoded = try Message.deserialize(arena.allocator(), expected, &read_pos, version);
    try testing.expectEqual(expected.len, read_pos);
    try testing.expectEqual(expected.len, decoded.calcSize(version));

    const decoded_buf = try testing.allocator.alloc(u8, expected.len);
    defer testing.allocator.free(decoded_buf);

    var decoded_pos: usize = 0;
    decoded.serialize(decoded_buf, &decoded_pos, version);
    try testing.expectEqual(expected.len, decoded_pos);
    try testing.expectEqualSlices(u8, expected, decoded_buf);
}

test "generated non-default golden fixtures cover legacy and flexible wire encodings" {
    {
        const RequestHeader = generated.request_header.RequestHeader;
        const value = RequestHeader{
            .request_api_key = 3,
            .request_api_version = 12,
            .correlation_id = 0x01020304,
            .client_id = "zig-client",
        };
        try expectGoldenRoundTrip(RequestHeader, value, 1, &[_]u8{
            0x00, 0x03, 0x00, 0x0c, 0x01, 0x02, 0x03, 0x04,
            0x00, 0x0a, 'z',  'i',  'g',  '-',  'c',  'l',
            'i',  'e',  'n',  't',
        });
        try expectGoldenRoundTrip(RequestHeader, value, 2, &[_]u8{
            0x00, 0x03, 0x00, 0x0c, 0x01, 0x02, 0x03, 0x04,
            0x0b, 'z',  'i',  'g',  '-',  'c',  'l',  'i',
            'e',  'n',  't',  0x00,
        });
    }

    {
        const MetadataRequest = generated.metadata_request.MetadataRequest;
        const topics = [_]MetadataRequest.MetadataRequestTopic{
            .{ .name = "alpha" },
            .{ .name = "beta" },
        };
        const value = MetadataRequest{
            .topics = &topics,
            .allow_auto_topic_creation = false,
            .include_cluster_authorized_operations = true,
            .include_topic_authorized_operations = false,
        };
        try expectGoldenRoundTrip(MetadataRequest, value, 8, &[_]u8{
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x05, 'a',  'l',
            'p',  'h',  'a',  0x00,
            0x04, 'b',  'e',  't',
            'a',  0x00, 0x01, 0x00,
        });
    }

    {
        const MetadataRequest = generated.metadata_request.MetadataRequest;
        const value = MetadataRequest{
            .topics = null,
        };
        try expectGoldenRoundTrip(MetadataRequest, value, 1, &[_]u8{
            0xff, 0xff, 0xff, 0xff,
        });
    }

    {
        const MetadataRequest = generated.metadata_request.MetadataRequest;
        const topics = [_]MetadataRequest.MetadataRequestTopic{
            .{
                .topic_id = .{
                    0x00, 0x01, 0x02, 0x03,
                    0x04, 0x05, 0x06, 0x07,
                    0x08, 0x09, 0x0a, 0x0b,
                    0x0c, 0x0d, 0x0e, 0x0f,
                },
                .name = "alpha",
            },
        };
        const value = MetadataRequest{
            .topics = &topics,
            .allow_auto_topic_creation = false,
            .include_cluster_authorized_operations = true,
            .include_topic_authorized_operations = true,
        };
        try expectGoldenRoundTrip(MetadataRequest, value, 10, &[_]u8{
            0x02,
            0x00,
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,
            0x09,
            0x0a,
            0x0b,
            0x0c,
            0x0d,
            0x0e,
            0x0f,
            0x06,
            'a',
            'l',
            'p',
            'h',
            'a',
            0x00,
            0x00,
            0x01,
            0x01,
            0x00,
        });
    }

    {
        const StopReplicaRequest = generated.stop_replica_request.StopReplicaRequest;
        const ungrouped_partitions = [_]StopReplicaRequest.StopReplicaPartitionV0{.{
            .topic_name = "topic-a",
            .partition_index = 2,
        }};
        const value = StopReplicaRequest{
            .controller_id = 7,
            .controller_epoch = 3,
            .delete_partitions = true,
            .ungrouped_partitions = &ungrouped_partitions,
        };
        try expectGoldenRoundTrip(StopReplicaRequest, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x03,
            0x01, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x00, 0x00,
            0x00, 0x02,
        });
    }

    {
        const StopReplicaRequest = generated.stop_replica_request.StopReplicaRequest;
        const topics = [_]StopReplicaRequest.StopReplicaTopicV1{.{
            .name = "topic-b",
            .partition_indexes = &[_]i32{ 1, 3 },
        }};
        const value = StopReplicaRequest{
            .controller_id = 7,
            .controller_epoch = 3,
            .broker_epoch = 9,
            .delete_partitions = true,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(StopReplicaRequest, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x09,
            0x01, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'b',  0x00, 0x00,
            0x00, 0x02, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00,
            0x00, 0x03,
        });
    }

    {
        const StopReplicaRequest = generated.stop_replica_request.StopReplicaRequest;
        const partition_states = [_]StopReplicaRequest.StopReplicaTopicState.StopReplicaPartitionState{.{
            .partition_index = 4,
            .leader_epoch = 11,
            .delete_partition = true,
        }};
        const topic_states = [_]StopReplicaRequest.StopReplicaTopicState{.{
            .topic_name = "topic-c",
            .partition_states = &partition_states,
        }};
        const value = StopReplicaRequest{
            .controller_id = 7,
            .controller_epoch = 3,
            .broker_epoch = 9,
            .delete_partitions = true,
            .topic_states = &topic_states,
        };
        try expectGoldenRoundTrip(StopReplicaRequest, value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x09,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'c',  0x02, 0x00, 0x00,
            0x00, 0x04, 0x00, 0x00,
            0x00, 0x0b, 0x01, 0x00,
            0x00, 0x00,
        });
    }

    {
        const AlterPartitionRequest = generated.alter_partition_request.AlterPartitionRequest;
        const partitions = [_]AlterPartitionRequest.TopicData.PartitionData{.{
            .partition_index = 2,
            .leader_epoch = 5,
            .new_isr = &[_]i32{ 1, 3 },
            .leader_recovery_state = 1,
            .partition_epoch = 12,
        }};
        const topics = [_]AlterPartitionRequest.TopicData{.{
            .topic_name = "topic-a",
            .partitions = &partitions,
        }};
        const value = AlterPartitionRequest{
            .broker_id = 7,
            .broker_epoch = 9,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(AlterPartitionRequest, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x09,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00,
            0x00, 0x05, 0x03, 0x00,
            0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x03, 0x01,
            0x00, 0x00, 0x00, 0x0c,
            0x00, 0x00, 0x00,
        });
    }

    {
        const AlterPartitionRequest = generated.alter_partition_request.AlterPartitionRequest;
        const topic_id = [_]u8{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        };
        const broker_states = [_]AlterPartitionRequest.TopicData.PartitionData.BrokerState{.{
            .broker_id = 1,
            .broker_epoch = 8,
        }};
        const partitions = [_]AlterPartitionRequest.TopicData.PartitionData{.{
            .partition_index = 2,
            .leader_epoch = 5,
            .new_isr = &[_]i32{99},
            .new_isr_with_epochs = &broker_states,
            .leader_recovery_state = 1,
            .partition_epoch = 12,
        }};
        const topics = [_]AlterPartitionRequest.TopicData{.{
            .topic_name = "legacy-name",
            .topic_id = topic_id,
            .partitions = &partitions,
        }};
        const value = AlterPartitionRequest{
            .broker_id = 7,
            .broker_epoch = 9,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(AlterPartitionRequest, value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x09,
            0x02, 0x00, 0x01, 0x02,
            0x03, 0x04, 0x05, 0x06,
            0x07, 0x08, 0x09, 0x0a,
            0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x02, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00,
            0x00, 0x05, 0x02, 0x00,
            0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x08, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x0c, 0x00, 0x00, 0x00,
        });
    }

    {
        const LeaderAndIsrRequest = generated.leader_and_isr_request.LeaderAndIsrRequest;
        const partitions = [_]generated.leader_and_isr_request.LeaderAndIsrPartitionState{.{
            .topic_name = "topic-a",
            .partition_index = 2,
            .controller_epoch = 3,
            .leader = 1,
            .leader_epoch = 5,
            .isr = &[_]i32{ 1, 2 },
            .partition_epoch = 6,
            .replicas = &[_]i32{ 1, 2, 3 },
            .is_new = true,
        }};
        const live_leaders = [_]LeaderAndIsrRequest.LeaderAndIsrLiveLeader{.{
            .broker_id = 1,
            .host_name = "b1",
            .port = 9092,
        }};
        const value = LeaderAndIsrRequest{
            .controller_id = 7,
            .controller_epoch = 3,
            .ungrouped_partition_states = &partitions,
            .live_leaders = &live_leaders,
        };
        try expectGoldenRoundTrip(LeaderAndIsrRequest, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x07, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x05, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x06, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x03, 0x01, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x02,
            'b',  '1',  0x00, 0x00,
            0x23, 0x84,
        });
    }

    {
        const LeaderAndIsrRequest = generated.leader_and_isr_request.LeaderAndIsrRequest;
        const topic_id = [_]u8{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        };
        const partitions = [_]generated.leader_and_isr_request.LeaderAndIsrPartitionState{.{
            .topic_name = "legacy-name",
            .partition_index = 2,
            .controller_epoch = 3,
            .leader = 1,
            .leader_epoch = 5,
            .isr = &[_]i32{ 1, 2 },
            .partition_epoch = 6,
            .replicas = &[_]i32{ 1, 2, 3 },
            .adding_replicas = &[_]i32{4},
            .removing_replicas = &[_]i32{5},
            .is_new = true,
            .leader_recovery_state = 1,
        }};
        const topics = [_]LeaderAndIsrRequest.LeaderAndIsrTopicState{.{
            .topic_name = "topic-b",
            .topic_id = topic_id,
            .partition_states = &partitions,
        }};
        const live_leaders = [_]LeaderAndIsrRequest.LeaderAndIsrLiveLeader{.{
            .broker_id = 1,
            .host_name = "b1",
            .port = 9092,
        }};
        const value = LeaderAndIsrRequest{
            .controller_id = 7,
            .controller_epoch = 3,
            .broker_epoch = 9,
            .type = 1,
            .ungrouped_partition_states = &partitions,
            .topic_states = &topics,
            .live_leaders = &live_leaders,
        };
        try expectGoldenRoundTrip(LeaderAndIsrRequest, value, 6, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x09,
            0x01, 0x02, 0x08, 't',
            'o',  'p',  'i',  'c',
            '-',  'b',  0x00, 0x01,
            0x02, 0x03, 0x04, 0x05,
            0x06, 0x07, 0x08, 0x09,
            0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x02, 0x00,
            0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x00,
            0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x05, 0x03,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x06,
            0x04, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x03, 0x02, 0x00, 0x00,
            0x00, 0x04, 0x02, 0x00,
            0x00, 0x00, 0x05, 0x01,
            0x01, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x01,
            0x03, 'b',  '1',  0x00,
            0x00, 0x23, 0x84, 0x00,
            0x00,
        });
    }

    {
        const LeaderAndIsrResponse = generated.leader_and_isr_response.LeaderAndIsrResponse;
        const partition_errors = [_]generated.leader_and_isr_response.LeaderAndIsrPartitionError{.{
            .topic_name = "topic-a",
            .partition_index = 2,
            .error_code = 6,
        }};
        const value = LeaderAndIsrResponse{
            .error_code = 0,
            .partition_errors = &partition_errors,
        };
        try expectGoldenRoundTrip(LeaderAndIsrResponse, value, 4, &[_]u8{
            0x00, 0x00, 0x02, 0x08,
            't',  'o',  'p',  'i',
            'c',  '-',  'a',  0x00,
            0x00, 0x00, 0x02, 0x00,
            0x06, 0x00, 0x00,
        });
    }

    {
        const LeaderAndIsrResponse = generated.leader_and_isr_response.LeaderAndIsrResponse;
        const topic_id = [_]u8{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        };
        const ignored_partition_errors = [_]generated.leader_and_isr_response.LeaderAndIsrPartitionError{.{
            .topic_name = "ignored",
            .partition_index = 99,
            .error_code = 6,
        }};
        const topic_partition_errors = [_]generated.leader_and_isr_response.LeaderAndIsrPartitionError{.{
            .topic_name = "legacy-name",
            .partition_index = 2,
            .error_code = 6,
        }};
        const topics = [_]LeaderAndIsrResponse.LeaderAndIsrTopicError{.{
            .topic_id = topic_id,
            .partition_errors = &topic_partition_errors,
        }};
        const value = LeaderAndIsrResponse{
            .error_code = 0,
            .partition_errors = &ignored_partition_errors,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(LeaderAndIsrResponse, value, 5, &[_]u8{
            0x00, 0x00, 0x02, 0x00,
            0x01, 0x02, 0x03, 0x04,
            0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c,
            0x0d, 0x0e, 0x0f, 0x02,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x06, 0x00, 0x00,
            0x00,
        });
    }

    {
        const DescribeConfigsRequest = generated.describe_configs_request.DescribeConfigsRequest;

        const legacy_null_resources = [_]DescribeConfigsRequest.DescribeConfigsResource{.{
            .resource_type = 2,
            .resource_name = "r",
            .configuration_keys = null,
        }};
        const legacy_null_value = DescribeConfigsRequest{
            .resources = &legacy_null_resources,
        };
        try expectGoldenRoundTrip(DescribeConfigsRequest, legacy_null_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x01, 'r',
            0xff, 0xff, 0xff, 0xff,
        });
        try expectGoldenRoundTrip(DescribeConfigsRequest, legacy_null_value, 4, &[_]u8{
            0x02, 0x02, 0x02, 'r', 0x00, 0x00, 0x00, 0x00,
            0x00,
        });

        const flexible_empty_resources = [_]DescribeConfigsRequest.DescribeConfigsResource{.{
            .resource_type = 2,
            .resource_name = "r",
            .configuration_keys = &.{},
        }};
        const flexible_empty_value = DescribeConfigsRequest{
            .resources = &flexible_empty_resources,
            .include_synonyms = false,
            .include_documentation = false,
        };
        try expectGoldenRoundTrip(DescribeConfigsRequest, flexible_empty_value, 4, &[_]u8{
            0x02, 0x02, 0x02, 'r', 0x01, 0x00, 0x00, 0x00,
            0x00,
        });
    }

    {
        const DescribeDelegationTokenRequest = generated.describe_delegation_token_request.DescribeDelegationTokenRequest;
        const null_value = DescribeDelegationTokenRequest{
            .owners = null,
        };
        try expectGoldenRoundTrip(DescribeDelegationTokenRequest, null_value, 0, &[_]u8{
            0xff, 0xff, 0xff, 0xff,
        });
        try expectGoldenRoundTrip(DescribeDelegationTokenRequest, null_value, 2, &[_]u8{
            0x00, 0x00,
        });

        const empty_owners = [_]DescribeDelegationTokenRequest.DescribeDelegationTokenOwner{};
        const empty_value = DescribeDelegationTokenRequest{
            .owners = &empty_owners,
        };
        try expectGoldenRoundTrip(DescribeDelegationTokenRequest, empty_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
        });
        try expectGoldenRoundTrip(DescribeDelegationTokenRequest, empty_value, 2, &[_]u8{
            0x01, 0x00,
        });
    }

    {
        const DescribeLogDirsRequest = generated.describe_log_dirs_request.DescribeLogDirsRequest;
        const null_value = DescribeLogDirsRequest{
            .topics = null,
        };
        try expectGoldenRoundTrip(DescribeLogDirsRequest, null_value, 0, &[_]u8{
            0xff, 0xff, 0xff, 0xff,
        });
        try expectGoldenRoundTrip(DescribeLogDirsRequest, null_value, 2, &[_]u8{
            0x00, 0x00,
        });

        const empty_value = DescribeLogDirsRequest{
            .topics = &.{},
        };
        try expectGoldenRoundTrip(DescribeLogDirsRequest, empty_value, 2, &[_]u8{
            0x01, 0x00,
        });
    }

    {
        const DescribeUserScramCredentialsRequest = generated.describe_user_scram_credentials_request.DescribeUserScramCredentialsRequest;
        const null_value = DescribeUserScramCredentialsRequest{
            .users = null,
        };
        try expectGoldenRoundTrip(DescribeUserScramCredentialsRequest, null_value, 0, &[_]u8{
            0x00, 0x00,
        });

        const empty_users = [_]DescribeUserScramCredentialsRequest.UserName{};
        const empty_value = DescribeUserScramCredentialsRequest{
            .users = &empty_users,
        };
        try expectGoldenRoundTrip(DescribeUserScramCredentialsRequest, empty_value, 0, &[_]u8{
            0x01, 0x00,
        });
    }

    {
        const DescribeUserScramCredentialsResponse = generated.describe_user_scram_credentials_response.DescribeUserScramCredentialsResponse;
        const CredentialInfo = DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult.CredentialInfo;
        const alice_credentials = [_]CredentialInfo{
            .{ .mechanism = 1, .iterations = 4096 },
            .{ .mechanism = 2, .iterations = 8192 },
        };
        const missing_credentials = [_]CredentialInfo{};
        const results = [_]DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult{
            .{
                .user = "alice",
                .credential_infos = &alice_credentials,
            },
            .{
                .user = "missing",
                .error_code = 42,
                .error_message = "unknown",
                .credential_infos = &missing_credentials,
            },
        };
        const value = DescribeUserScramCredentialsResponse{
            .throttle_time_ms = 4,
            .results = &results,
        };
        try expectGoldenRoundTrip(DescribeUserScramCredentialsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x04,
            0x00, 0x00, 0x00, 0x03,
            0x06, 'a',  'l',  'i',
            'c',  'e',  0x00, 0x00,
            0x00, 0x03, 0x01, 0x00,
            0x00, 0x10, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x20,
            0x00, 0x00, 0x00, 0x08,
            'm',  'i',  's',  's',
            'i',  'n',  'g',  0x00,
            0x2a, 0x08, 'u',  'n',
            'k',  'n',  'o',  'w',
            'n',  0x01, 0x00, 0x00,
        });
    }

    {
        const AlterUserScramCredentialsRequest = generated.alter_user_scram_credentials_request.AlterUserScramCredentialsRequest;
        const deletions = [_]AlterUserScramCredentialsRequest.ScramCredentialDeletion{.{
            .name = "bob",
            .mechanism = 1,
        }};
        const upsertions = [_]AlterUserScramCredentialsRequest.ScramCredentialUpsertion{.{
            .name = "alice",
            .mechanism = 1,
            .iterations = 4096,
            .salt = &[_]u8{ 0xaa, 0xbb, 0xcc },
            .salted_password = &[_]u8{ 0x01, 0x02, 0x03, 0x04 },
        }};
        const value = AlterUserScramCredentialsRequest{
            .deletions = &deletions,
            .upsertions = &upsertions,
        };
        try expectGoldenRoundTrip(AlterUserScramCredentialsRequest, value, 0, &[_]u8{
            0x02, 0x04, 'b',  'o',
            'b',  0x01, 0x00, 0x02,
            0x06, 'a',  'l',  'i',
            'c',  'e',  0x01, 0x00,
            0x00, 0x10, 0x00, 0x04,
            0xaa, 0xbb, 0xcc, 0x05,
            0x01, 0x02, 0x03, 0x04,
            0x00, 0x00,
        });
    }

    {
        const AlterUserScramCredentialsResponse = generated.alter_user_scram_credentials_response.AlterUserScramCredentialsResponse;
        const results = [_]AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult{
            .{ .user = "alice" },
            .{ .user = "bob", .error_code = 42, .error_message = "missing" },
        };
        const value = AlterUserScramCredentialsResponse{
            .throttle_time_ms = 6,
            .results = &results,
        };
        try expectGoldenRoundTrip(AlterUserScramCredentialsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x06,
            0x03, 0x06, 'a',  'l',
            'i',  'c',  'e',  0x00,
            0x00, 0x00, 0x00, 0x04,
            'b',  'o',  'b',  0x00,
            0x2a, 0x08, 'm',  'i',
            's',  's',  'i',  'n',
            'g',  0x00, 0x00,
        });
    }

    {
        const DescribeAclsRequest = generated.describe_acls_request.DescribeAclsRequest;
        const value = DescribeAclsRequest{
            .resource_type_filter = 2,
            .resource_name_filter = "topic-a",
            .pattern_type_filter = 3,
            .principal_filter = "User:alice",
            .host_filter = "*",
            .operation = 3,
            .permission_type = 3,
        };
        try expectGoldenRoundTrip(DescribeAclsRequest, value, 2, &[_]u8{
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x03, 0x0b, 'U',
            's',  'e',  'r',  ':',
            'a',  'l',  'i',  'c',
            'e',  0x02, '*',  0x03,
            0x03, 0x00,
        });
    }

    {
        const DescribeAclsResponse = generated.describe_acls_response.DescribeAclsResponse;
        const AclDescription = DescribeAclsResponse.DescribeAclsResource.AclDescription;
        const acls = [_]AclDescription{.{
            .principal = "User:alice",
            .host = "*",
            .operation = 3,
            .permission_type = 3,
        }};
        const resources = [_]DescribeAclsResponse.DescribeAclsResource{.{
            .resource_type = 2,
            .resource_name = "topic-a",
            .pattern_type = 3,
            .acls = &acls,
        }};
        const value = DescribeAclsResponse{
            .throttle_time_ms = 5,
            .resources = &resources,
        };
        try expectGoldenRoundTrip(DescribeAclsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x05,
            0x00, 0x00, 0x00, 0x02,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x03, 0x02, 0x0b,
            'U',  's',  'e',  'r',
            ':',  'a',  'l',  'i',
            'c',  'e',  0x02, '*',
            0x03, 0x03, 0x00, 0x00,
            0x00,
        });
    }

    {
        const CreateAclsRequest = generated.create_acls_request.CreateAclsRequest;
        const creations = [_]CreateAclsRequest.AclCreation{.{
            .resource_type = 2,
            .resource_name = "topic-a",
            .resource_pattern_type = 3,
            .principal = "User:alice",
            .host = "*",
            .operation = 4,
            .permission_type = 3,
        }};
        const value = CreateAclsRequest{
            .creations = &creations,
        };
        try expectGoldenRoundTrip(CreateAclsRequest, value, 2, &[_]u8{
            0x02, 0x02, 0x08, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x03, 0x0b,
            'U',  's',  'e',  'r',
            ':',  'a',  'l',  'i',
            'c',  'e',  0x02, '*',
            0x04, 0x03, 0x00, 0x00,
        });
    }

    {
        const CreateAclsResponse = generated.create_acls_response.CreateAclsResponse;
        const results = [_]CreateAclsResponse.AclCreationResult{
            .{},
            .{ .error_code = 42, .error_message = "denied" },
        };
        const value = CreateAclsResponse{
            .throttle_time_ms = 6,
            .results = &results,
        };
        try expectGoldenRoundTrip(CreateAclsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x06,
            0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x2a, 0x07,
            'd',  'e',  'n',  'i',
            'e',  'd',  0x00, 0x00,
        });
    }

    {
        const DeleteAclsRequest = generated.delete_acls_request.DeleteAclsRequest;
        const filters = [_]DeleteAclsRequest.DeleteAclsFilter{.{
            .resource_type_filter = 2,
            .resource_name_filter = "topic-a",
            .pattern_type_filter = 3,
            .principal_filter = null,
            .host_filter = "*",
            .operation = 3,
            .permission_type = 3,
        }};
        const value = DeleteAclsRequest{
            .filters = &filters,
        };
        try expectGoldenRoundTrip(DeleteAclsRequest, value, 2, &[_]u8{
            0x02, 0x02, 0x08, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x03, 0x00,
            0x02, '*',  0x03, 0x03,
            0x00, 0x00,
        });
    }

    {
        const DeleteAclsResponse = generated.delete_acls_response.DeleteAclsResponse;
        const MatchingAcl = DeleteAclsResponse.DeleteAclsFilterResult.DeleteAclsMatchingAcl;
        const matching_acls = [_]MatchingAcl{.{
            .resource_type = 2,
            .resource_name = "topic-a",
            .pattern_type = 3,
            .principal = "User:alice",
            .host = "*",
            .operation = 3,
            .permission_type = 3,
        }};
        const filter_results = [_]DeleteAclsResponse.DeleteAclsFilterResult{.{
            .matching_acls = &matching_acls,
        }};
        const value = DeleteAclsResponse{
            .throttle_time_ms = 7,
            .filter_results = &filter_results,
        };
        try expectGoldenRoundTrip(DeleteAclsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x02, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x03, 0x0b, 'U',
            's',  'e',  'r',  ':',
            'a',  'l',  'i',  'c',
            'e',  0x02, '*',  0x03,
            0x03, 0x00, 0x00, 0x00,
        });
    }

    {
        const SaslHandshakeRequest = generated.sasl_handshake_request.SaslHandshakeRequest;
        const value = SaslHandshakeRequest{
            .mechanism = "SCRAM-SHA-256",
        };
        try expectGoldenRoundTrip(SaslHandshakeRequest, value, 1, &[_]u8{
            0x00, 0x0d, 'S', 'C',
            'R',  'A',  'M', '-',
            'S',  'H',  'A', '-',
            '2',  '5',  '6',
        });
    }

    {
        const SaslHandshakeResponse = generated.sasl_handshake_response.SaslHandshakeResponse;
        const mechanisms = [_]?[]const u8{ "PLAIN", "SCRAM-SHA-256" };
        const value = SaslHandshakeResponse{
            .mechanisms = &mechanisms,
        };
        try expectGoldenRoundTrip(SaslHandshakeResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x05,
            'P',  'L',  'A',  'I',
            'N',  0x00, 0x0d, 'S',
            'C',  'R',  'A',  'M',
            '-',  'S',  'H',  'A',
            '-',  '2',  '5',  '6',
        });
    }

    {
        const SaslAuthenticateRequest = generated.sasl_authenticate_request.SaslAuthenticateRequest;
        const value = SaslAuthenticateRequest{
            .auth_bytes = &[_]u8{ 0x01, 0x02, 0x03 },
        };
        try expectGoldenRoundTrip(SaslAuthenticateRequest, value, 2, &[_]u8{
            0x04, 0x01, 0x02, 0x03,
            0x00,
        });
    }

    {
        const SaslAuthenticateResponse = generated.sasl_authenticate_response.SaslAuthenticateResponse;
        const value = SaslAuthenticateResponse{
            .error_code = 33,
            .error_message = "bad auth",
            .auth_bytes = &[_]u8{ 0xaa, 0xbb },
            .session_lifetime_ms = 60000,
        };
        try expectGoldenRoundTrip(SaslAuthenticateResponse, value, 2, &[_]u8{
            0x00, 0x21, 0x09, 'b',
            'a',  'd',  ' ',  'a',
            'u',  't',  'h',  0x03,
            0xaa, 0xbb, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0xea, 0x60, 0x00,
        });
    }

    {
        const ElectLeadersRequest = generated.elect_leaders_request.ElectLeadersRequest;
        const null_value = ElectLeadersRequest{
            .election_type = 0,
            .topic_partitions = null,
            .timeout_ms = 1000,
        };
        try expectGoldenRoundTrip(ElectLeadersRequest, null_value, 0, &[_]u8{
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x03, 0xe8,
        });
        try expectGoldenRoundTrip(ElectLeadersRequest, null_value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x00,
        });

        const empty_value = ElectLeadersRequest{
            .election_type = 0,
            .topic_partitions = &.{},
            .timeout_ms = 1000,
        };
        try expectGoldenRoundTrip(ElectLeadersRequest, empty_value, 2, &[_]u8{
            0x00, 0x01, 0x00, 0x00, 0x03, 0xe8, 0x00,
        });
    }

    {
        const CreatePartitionsRequest = generated.create_partitions_request.CreatePartitionsRequest;
        const null_topics = [_]CreatePartitionsRequest.CreatePartitionsTopic{.{
            .name = "p",
            .count = 2,
            .assignments = null,
        }};
        const null_value = CreatePartitionsRequest{
            .topics = &null_topics,
            .timeout_ms = 1000,
            .validate_only = false,
        };
        try expectGoldenRoundTrip(CreatePartitionsRequest, null_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x01, 'p',  0x00,
            0x00, 0x00, 0x02, 0xff,
            0xff, 0xff, 0xff, 0x00,
            0x00, 0x03, 0xe8, 0x00,
        });
        try expectGoldenRoundTrip(CreatePartitionsRequest, null_value, 2, &[_]u8{
            0x02, 0x02, 'p',
            0x00, 0x00, 0x00,
            0x02, 0x00, 0x00,
            0x00, 0x00, 0x03,
            0xe8, 0x00, 0x00,
        });

        const empty_topics = [_]CreatePartitionsRequest.CreatePartitionsTopic{.{
            .name = "p",
            .count = 2,
            .assignments = &.{},
        }};
        const empty_value = CreatePartitionsRequest{
            .topics = &empty_topics,
            .timeout_ms = 1000,
            .validate_only = false,
        };
        try expectGoldenRoundTrip(CreatePartitionsRequest, empty_value, 2, &[_]u8{
            0x02, 0x02, 'p',
            0x00, 0x00, 0x00,
            0x02, 0x01, 0x00,
            0x00, 0x00, 0x03,
            0xe8, 0x00, 0x00,
        });
    }

    {
        const DescribeConfigsResponse = generated.describe_configs_response.DescribeConfigsResponse;
        const synonyms = [_]DescribeConfigsResponse.DescribeConfigsResult.DescribeConfigsResourceResult.DescribeConfigsSynonym{.{
            .name = "cleanup.policy",
            .value = "delete",
            .source = 1,
        }};
        const configs = [_]DescribeConfigsResponse.DescribeConfigsResult.DescribeConfigsResourceResult{.{
            .name = "cleanup.policy",
            .value = "compact",
            .read_only = true,
            .is_default = true,
            .config_source = 5,
            .is_sensitive = false,
            .synonyms = &synonyms,
        }};
        const results = [_]DescribeConfigsResponse.DescribeConfigsResult{.{
            .error_code = 0,
            .error_message = null,
            .resource_type = 2,
            .resource_name = "topic-a",
            .configs = &configs,
        }};
        const value = DescribeConfigsResponse{
            .throttle_time_ms = 5,
            .results = &results,
        };
        try expectGoldenRoundTrip(DescribeConfigsResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x05,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0xff, 0xff,
            0x02, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x00, 0x00,
            0x00, 0x01, 0x00, 0x0e,
            'c',  'l',  'e',  'a',
            'n',  'u',  'p',  '.',
            'p',  'o',  'l',  'i',
            'c',  'y',  0x00, 0x07,
            'c',  'o',  'm',  'p',
            'a',  'c',  't',  0x01,
            0x05, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x0e,
            'c',  'l',  'e',  'a',
            'n',  'u',  'p',  '.',
            'p',  'o',  'l',  'i',
            'c',  'y',  0x00, 0x06,
            'd',  'e',  'l',  'e',
            't',  'e',  0x01,
        });
    }

    {
        const AlterConfigsRequest = generated.alter_configs_request.AlterConfigsRequest;
        const configs = [_]AlterConfigsRequest.AlterConfigsResource.AlterableConfig{
            .{
                .name = "retention.ms",
                .value = "60000",
            },
            .{
                .name = "cleanup.policy",
                .value = null,
            },
        };
        const resources = [_]AlterConfigsRequest.AlterConfigsResource{.{
            .resource_type = 2,
            .resource_name = "topic-a",
            .configs = &configs,
        }};
        const value = AlterConfigsRequest{
            .resources = &resources,
            .validate_only = true,
        };
        try expectGoldenRoundTrip(AlterConfigsRequest, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x01,
            0x02, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x00, 0x00,
            0x00, 0x02, 0x00, 0x0c,
            'r',  'e',  't',  'e',
            'n',  't',  'i',  'o',
            'n',  '.',  'm',  's',
            0x00, 0x05, '6',  '0',
            '0',  '0',  '0',  0x00,
            0x0e, 'c',  'l',  'e',
            'a',  'n',  'u',  'p',
            '.',  'p',  'o',  'l',
            'i',  'c',  'y',  0xff,
            0xff, 0x01,
        });
        try expectGoldenRoundTrip(AlterConfigsRequest, value, 2, &[_]u8{
            0x02,
            0x02,
            0x08,
            't',
            'o',
            'p',
            'i',
            'c',
            '-',
            'a',
            0x03,
            0x0d,
            'r',
            'e',
            't',
            'e',
            'n',
            't',
            'i',
            'o',
            'n',
            '.',
            'm',
            's',
            0x06,
            '6',
            '0',
            '0',
            '0',
            '0',
            0x00,
            0x0f,
            'c',
            'l',
            'e',
            'a',
            'n',
            'u',
            'p',
            '.',
            'p',
            'o',
            'l',
            'i',
            'c',
            'y',
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
        });
    }

    {
        const AlterConfigsResponse = generated.alter_configs_response.AlterConfigsResponse;
        const responses = [_]AlterConfigsResponse.AlterConfigsResourceResponse{
            .{
                .error_code = 0,
                .error_message = null,
                .resource_type = 2,
                .resource_name = "topic-a",
            },
            .{
                .error_code = 40,
                .error_message = "bad",
                .resource_type = 2,
                .resource_name = "topic-b",
            },
        };
        const value = AlterConfigsResponse{
            .throttle_time_ms = 7,
            .responses = &responses,
        };
        try expectGoldenRoundTrip(AlterConfigsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0xff, 0xff,
            0x02, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x00, 0x28,
            0x00, 0x03, 'b',  'a',
            'd',  0x02, 0x00, 0x07,
            't',  'o',  'p',  'i',
            'c',  '-',  'b',
        });
        try expectGoldenRoundTrip(AlterConfigsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x03, 0x00, 0x00, 0x00,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x00, 0x28,
            0x04, 'b',  'a',  'd',
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'b',  0x00, 0x00,
        });
    }

    {
        const IncrementalAlterConfigsRequest = generated.incremental_alter_configs_request.IncrementalAlterConfigsRequest;
        const configs = [_]IncrementalAlterConfigsRequest.AlterConfigsResource.AlterableConfig{
            .{
                .name = "retention.ms",
                .config_operation = 0,
                .value = "60000",
            },
            .{
                .name = "cleanup.policy",
                .config_operation = 1,
                .value = null,
            },
        };
        const resources = [_]IncrementalAlterConfigsRequest.AlterConfigsResource{.{
            .resource_type = 2,
            .resource_name = "topic-a",
            .configs = &configs,
        }};
        const value = IncrementalAlterConfigsRequest{
            .resources = &resources,
            .validate_only = true,
        };
        try expectGoldenRoundTrip(IncrementalAlterConfigsRequest, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x01,
            0x02, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x00, 0x00,
            0x00, 0x02, 0x00, 0x0c,
            'r',  'e',  't',  'e',
            'n',  't',  'i',  'o',
            'n',  '.',  'm',  's',
            0x00, 0x00, 0x05, '6',
            '0',  '0',  '0',  '0',
            0x00, 0x0e, 'c',  'l',
            'e',  'a',  'n',  'u',
            'p',  '.',  'p',  'o',
            'l',  'i',  'c',  'y',
            0x01, 0xff, 0xff, 0x01,
        });
        try expectGoldenRoundTrip(IncrementalAlterConfigsRequest, value, 1, &[_]u8{
            0x02,
            0x02,
            0x08,
            't',
            'o',
            'p',
            'i',
            'c',
            '-',
            'a',
            0x03,
            0x0d,
            'r',
            'e',
            't',
            'e',
            'n',
            't',
            'i',
            'o',
            'n',
            '.',
            'm',
            's',
            0x00,
            0x06,
            '6',
            '0',
            '0',
            '0',
            '0',
            0x00,
            0x0f,
            'c',
            'l',
            'e',
            'a',
            'n',
            'u',
            'p',
            '.',
            'p',
            'o',
            'l',
            'i',
            'c',
            'y',
            0x01,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
        });
    }

    {
        const IncrementalAlterConfigsResponse = generated.incremental_alter_configs_response.IncrementalAlterConfigsResponse;
        const responses = [_]IncrementalAlterConfigsResponse.AlterConfigsResourceResponse{
            .{
                .error_code = 0,
                .error_message = null,
                .resource_type = 2,
                .resource_name = "topic-a",
            },
            .{
                .error_code = 40,
                .error_message = "bad",
                .resource_type = 2,
                .resource_name = "topic-b",
            },
        };
        const value = IncrementalAlterConfigsResponse{
            .throttle_time_ms = 7,
            .responses = &responses,
        };
        try expectGoldenRoundTrip(IncrementalAlterConfigsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0xff, 0xff,
            0x02, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x00, 0x28,
            0x00, 0x03, 'b',  'a',
            'd',  0x02, 0x00, 0x07,
            't',  'o',  'p',  'i',
            'c',  '-',  'b',
        });
        try expectGoldenRoundTrip(IncrementalAlterConfigsResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x03, 0x00, 0x00, 0x00,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x00, 0x28,
            0x04, 'b',  'a',  'd',
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'b',  0x00, 0x00,
        });
    }

    {
        const DeleteTopicsRequest = generated.delete_topics_request.DeleteTopicsRequest;
        const topic_names = [_]?[]const u8{ "topic-a", "topic-b" };
        const legacy_value = DeleteTopicsRequest{
            .topic_names = &topic_names,
            .timeout_ms = 30_000,
        };
        try expectGoldenRoundTrip(DeleteTopicsRequest, legacy_value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x07, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x07, 't',
            'o',  'p',  'i',  'c',
            '-',  'b',  0x00, 0x00,
            0x75, 0x30,
        });

        const topics = [_]DeleteTopicsRequest.DeleteTopicState{
            .{
                .name = "topic-a",
                .topic_id = .{
                    0x00, 0x01, 0x02, 0x03,
                    0x04, 0x05, 0x06, 0x07,
                    0x08, 0x09, 0x0a, 0x0b,
                    0x0c, 0x0d, 0x0e, 0x0f,
                },
            },
            .{
                .name = null,
                .topic_id = .{
                    0x10, 0x11, 0x12, 0x13,
                    0x14, 0x15, 0x16, 0x17,
                    0x18, 0x19, 0x1a, 0x1b,
                    0x1c, 0x1d, 0x1e, 0x1f,
                },
            },
        };
        const flexible_value = DeleteTopicsRequest{
            .topics = &topics,
            .timeout_ms = 30_000,
        };
        try expectGoldenRoundTrip(DeleteTopicsRequest, flexible_value, 6, &[_]u8{
            0x03,
            0x08,
            't',
            'o',
            'p',
            'i',
            'c',
            '-',
            'a',
            0x00,
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,
            0x09,
            0x0a,
            0x0b,
            0x0c,
            0x0d,
            0x0e,
            0x0f,
            0x00,
            0x00,
            0x10,
            0x11,
            0x12,
            0x13,
            0x14,
            0x15,
            0x16,
            0x17,
            0x18,
            0x19,
            0x1a,
            0x1b,
            0x1c,
            0x1d,
            0x1e,
            0x1f,
            0x00,
            0x00,
            0x00,
            0x75,
            0x30,
            0x00,
        });
    }

    {
        const DeleteTopicsResponse = generated.delete_topics_response.DeleteTopicsResponse;
        const responses = [_]DeleteTopicsResponse.DeletableTopicResult{
            .{
                .name = "topic-a",
                .topic_id = .{
                    0x00, 0x01, 0x02, 0x03,
                    0x04, 0x05, 0x06, 0x07,
                    0x08, 0x09, 0x0a, 0x0b,
                    0x0c, 0x0d, 0x0e, 0x0f,
                },
                .error_code = 0,
                .error_message = null,
            },
            .{
                .name = "topic-b",
                .topic_id = .{
                    0x10, 0x11, 0x12, 0x13,
                    0x14, 0x15, 0x16, 0x17,
                    0x18, 0x19, 0x1a, 0x1b,
                    0x1c, 0x1d, 0x1e, 0x1f,
                },
                .error_code = 3,
                .error_message = "missing",
            },
        };
        const value = DeleteTopicsResponse{
            .throttle_time_ms = 7,
            .responses = &responses,
        };
        try expectGoldenRoundTrip(DeleteTopicsResponse, value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x07, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x00, 0x00,
            0x07, 't',  'o',  'p',
            'i',  'c',  '-',  'b',
            0x00, 0x03,
        });
        try expectGoldenRoundTrip(DeleteTopicsResponse, value, 6, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x03, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x01, 0x02,
            0x03, 0x04, 0x05, 0x06,
            0x07, 0x08, 0x09, 0x0a,
            0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x00, 0x00, 0x00,
            0x00, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'b',  0x10, 0x11, 0x12,
            0x13, 0x14, 0x15, 0x16,
            0x17, 0x18, 0x19, 0x1a,
            0x1b, 0x1c, 0x1d, 0x1e,
            0x1f, 0x00, 0x03, 0x08,
            'm',  'i',  's',  's',
            'i',  'n',  'g',  0x00,
            0x00,
        });
    }

    {
        const DeleteGroupsRequest = generated.delete_groups_request.DeleteGroupsRequest;
        const groups = [_]?[]const u8{ "g1", "g2" };
        const value = DeleteGroupsRequest{
            .groups_names = &groups,
        };
        try expectGoldenRoundTrip(DeleteGroupsRequest, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x02, 'g',  '1',
            0x00, 0x02, 'g',  '2',
        });
        try expectGoldenRoundTrip(DeleteGroupsRequest, value, 2, &[_]u8{
            0x03,
            0x03,
            'g',
            '1',
            0x03,
            'g',
            '2',
            0x00,
        });
    }

    {
        const DeleteGroupsResponse = generated.delete_groups_response.DeleteGroupsResponse;
        const results = [_]DeleteGroupsResponse.DeletableGroupResult{
            .{ .group_id = "g1", .error_code = 0 },
            .{ .group_id = "g2", .error_code = 69 },
        };
        const value = DeleteGroupsResponse{
            .throttle_time_ms = 7,
            .results = &results,
        };
        try expectGoldenRoundTrip(DeleteGroupsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x02, 'g',  '1',
            0x00, 0x00, 0x00, 0x02,
            'g',  '2',  0x00, 0x45,
        });
        try expectGoldenRoundTrip(DeleteGroupsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x03, 0x03, 'g',  '1',
            0x00, 0x00, 0x00, 0x03,
            'g',  '2',  0x00, 0x45,
            0x00, 0x00,
        });
    }

    {
        const ProduceRequest = generated.produce_request.ProduceRequest;
        const partitions = [_]ProduceRequest.TopicProduceData.PartitionProduceData{
            .{ .index = 2, .records = &[_]u8{ 0x01, 0x02, 0x03 } },
        };
        const topics = [_]ProduceRequest.TopicProduceData{
            .{ .name = "topic-a", .partition_data = &partitions },
        };
        const value = ProduceRequest{
            .transactional_id = "txn-1",
            .acks = -1,
            .timeout_ms = 1500,
            .topic_data = &topics,
        };
        try expectGoldenRoundTrip(ProduceRequest, value, 9, &[_]u8{
            0x06, 't',  'x',  'n',  '-',  '1',
            0xff, 0xff, 0x00, 0x00, 0x05, 0xdc,
            0x02, 0x08, 't',  'o',  'p',  'i',
            'c',  '-',  'a',  0x02, 0x00, 0x00,
            0x00, 0x02, 0x04, 0x01, 0x02, 0x03,
            0x00, 0x00, 0x00,
        });
    }

    {
        const ProduceResponse = generated.produce_response.ProduceResponse;
        const PartitionResponse = ProduceResponse.TopicProduceResponse.PartitionProduceResponse;
        const topic_partitions = [_]PartitionResponse{.{
            .index = 1,
            .error_code = 6,
            .base_offset = -1,
            .log_append_time_ms = -1,
            .log_start_offset = -1,
            .current_leader = .{ .leader_id = 7, .leader_epoch = 3 },
        }};
        const topics = [_]ProduceResponse.TopicProduceResponse{.{
            .name = "topic-a",
            .partition_responses = &topic_partitions,
        }};
        const node_endpoints = [_]ProduceResponse.NodeEndpoint{.{
            .node_id = 7,
            .host = "broker-7",
            .port = 9092,
            .rack = "az-a",
        }};
        const value = ProduceResponse{
            .responses = &topics,
            .throttle_time_ms = 5,
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(ProduceResponse, value, 10, &[_]u8{
            0x02, 0x08, 't',  'o',  'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0x01, 0x00, 0x01, 0x00, 0x09, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x05, 0x01, 0x00, 0x18, 0x02, 0x00,
            0x00, 0x00, 0x07, 0x09, 'b',  'r',  'o',  'k',
            'e',  'r',  '-',  '7',  0x00, 0x00, 0x23, 0x84,
            0x05, 'a',  'z',  '-',  'a',  0x00,
        });
    }

    {
        const FetchResponse = generated.fetch_response.FetchResponse;
        const PartitionData = FetchResponse.FetchableTopicResponse.PartitionData;
        const null_partitions = [_]PartitionData{.{
            .partition_index = 0,
            .error_code = 0,
            .high_watermark = 0,
            .last_stable_offset = -1,
            .aborted_transactions = null,
            .records = null,
        }};
        const null_topics = [_]FetchResponse.FetchableTopicResponse{.{
            .topic = "t",
            .partitions = &null_partitions,
        }};
        const null_value = FetchResponse{
            .throttle_time_ms = 0,
            .error_code = 0,
            .session_id = 0,
            .responses = &null_topics,
        };
        try expectGoldenRoundTrip(FetchResponse, null_value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x01, 't',  0x00,
            0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff,
        });
        try expectGoldenRoundTrip(FetchResponse, null_value, 12, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x02,
            't',  0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0x00, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x00,
            0x00,
        });

        const empty_aborted = [_]PartitionData.AbortedTransaction{};
        const empty_partitions = [_]PartitionData{.{
            .partition_index = 0,
            .error_code = 0,
            .high_watermark = 0,
            .last_stable_offset = -1,
            .aborted_transactions = &empty_aborted,
            .records = null,
        }};
        const empty_topics = [_]FetchResponse.FetchableTopicResponse{.{
            .topic = "t",
            .partitions = &empty_partitions,
        }};
        const empty_value = FetchResponse{
            .throttle_time_ms = 0,
            .error_code = 0,
            .session_id = 0,
            .responses = &empty_topics,
        };
        try expectGoldenRoundTrip(FetchResponse, empty_value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x01, 't',  0x00,
            0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x00,
            0x00, 0xff, 0xff, 0xff,
            0xff,
        });
        try expectGoldenRoundTrip(FetchResponse, empty_value, 12, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x02,
            't',  0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0x01, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x00,
            0x00,
        });
    }

    {
        const FetchResponse = generated.fetch_response.FetchResponse;
        const PartitionData = FetchResponse.FetchableTopicResponse.PartitionData;
        const empty_aborted = [_]PartitionData.AbortedTransaction{};
        const partitions = [_]PartitionData{.{
            .partition_index = 2,
            .error_code = 6,
            .high_watermark = 123,
            .last_stable_offset = 100,
            .log_start_offset = 5,
            .aborted_transactions = &empty_aborted,
            .preferred_read_replica = -1,
            .records = null,
            .diverging_epoch = .{ .epoch = 4, .end_offset = 44 },
            .current_leader = .{ .leader_id = 7, .leader_epoch = 3 },
            .snapshot_id = .{ .end_offset = 50, .epoch = 6 },
        }};
        const topics = [_]FetchResponse.FetchableTopicResponse{.{
            .topic_id = .{
                0x00, 0x01, 0x02, 0x03,
                0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b,
                0x0c, 0x0d, 0x0e, 0x0f,
            },
            .partitions = &partitions,
        }};
        const node_endpoints = [_]FetchResponse.NodeEndpoint{.{
            .node_id = 7,
            .host = "broker-7",
            .port = 9092,
            .rack = "az-a",
        }};
        const value = FetchResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .session_id = 99,
            .responses = &topics,
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(FetchResponse, value, 16, &[_]u8{
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x63, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04,
            0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
            0x0d, 0x0e, 0x0f, 0x02, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x7b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x05, 0x01, 0xff, 0xff, 0xff, 0xff, 0x00,
            0x03, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00,
            0x01, 0x09, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x02, 0x0d, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00,
            0x06, 0x00, 0x00, 0x01, 0x00, 0x18, 0x02, 0x00,
            0x00, 0x00, 0x07, 0x09, 'b',  'r',  'o',  'k',
            'e',  'r',  '-',  '7',  0x00, 0x00, 0x23, 0x84,
            0x05, 'a',  'z',  '-',  'a',  0x00,
        });
    }

    {
        const VoteResponse = generated.vote_response.VoteResponse;
        const node_endpoints = [_]VoteResponse.NodeEndpoint{.{
            .node_id = 7,
            .host = "n1",
            .port = 9092,
        }};
        const value = VoteResponse{
            .error_code = 0,
            .topics = &.{},
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(VoteResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x01, 0x01, 0x00, 0x0b, 0x02, 0x00,
            0x00, 0x00, 0x07, 0x03, 'n',  '1',  0x23, 0x84,
            0x00,
        });
    }

    {
        const BeginQuorumEpochResponse = generated.begin_quorum_epoch_response.BeginQuorumEpochResponse;
        const node_endpoints = [_]BeginQuorumEpochResponse.NodeEndpoint{.{
            .node_id = 3,
            .host = "bq",
            .port = 9093,
        }};
        const value = BeginQuorumEpochResponse{
            .error_code = 0,
            .topics = &.{},
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(BeginQuorumEpochResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x01, 0x01, 0x00, 0x0b, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x03, 'b',  'q',  0x23, 0x85,
            0x00,
        });
    }

    {
        const EndQuorumEpochResponse = generated.end_quorum_epoch_response.EndQuorumEpochResponse;
        const node_endpoints = [_]EndQuorumEpochResponse.NodeEndpoint{.{
            .node_id = 4,
            .host = "eq",
            .port = 9094,
        }};
        const value = EndQuorumEpochResponse{
            .error_code = 0,
            .topics = &.{},
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(EndQuorumEpochResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x01, 0x01, 0x00, 0x0b, 0x02, 0x00,
            0x00, 0x00, 0x04, 0x03, 'e',  'q',  0x23, 0x86,
            0x00,
        });
    }

    {
        const EndQuorumEpochRequest = generated.end_quorum_epoch_request.EndQuorumEpochRequest;
        const ignored_candidate_directory_id = [_]u8{
            0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        };
        const ignored_candidates = [_]EndQuorumEpochRequest.TopicData.PartitionData.ReplicaInfo{.{
            .candidate_id = 99,
            .candidate_directory_id = ignored_candidate_directory_id,
        }};
        const partitions = [_]EndQuorumEpochRequest.TopicData.PartitionData{.{
            .partition_index = 2,
            .leader_id = 7,
            .leader_epoch = 3,
            .preferred_successors = &[_]i32{ 4, 5 },
            .preferred_candidates = &ignored_candidates,
        }};
        const topics = [_]EndQuorumEpochRequest.TopicData{.{
            .topic_name = "topic-a",
            .partitions = &partitions,
        }};
        const ignored_leader_endpoints = [_]EndQuorumEpochRequest.LeaderEndpoint{.{
            .name = "listener",
            .host = "broker-7",
            .port = 9092,
        }};
        const value = EndQuorumEpochRequest{
            .cluster_id = "cluster-a",
            .topics = &topics,
            .leader_endpoints = &ignored_leader_endpoints,
        };
        try expectGoldenRoundTrip(EndQuorumEpochRequest, value, 0, &[_]u8{
            0x00, 0x09, 'c',  'l',  'u',  's',  't',  'e',
            'r',  '-',  'a',  0x00, 0x00, 0x00, 0x01, 0x00,
            0x07, 't',  'o',  'p',  'i',  'c',  '-',  'a',
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04,
            0x00, 0x00, 0x00, 0x05,
        });
    }

    {
        const EndQuorumEpochRequest = generated.end_quorum_epoch_request.EndQuorumEpochRequest;
        const candidate_directory_id = [_]u8{
            0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        };
        const preferred_candidates = [_]EndQuorumEpochRequest.TopicData.PartitionData.ReplicaInfo{.{
            .candidate_id = 4,
            .candidate_directory_id = candidate_directory_id,
        }};
        const partitions = [_]EndQuorumEpochRequest.TopicData.PartitionData{.{
            .partition_index = 2,
            .leader_id = 7,
            .leader_epoch = 3,
            .preferred_successors = &[_]i32{ 44, 55 },
            .preferred_candidates = &preferred_candidates,
        }};
        const topics = [_]EndQuorumEpochRequest.TopicData{.{
            .topic_name = "topic-a",
            .partitions = &partitions,
        }};
        const leader_endpoints = [_]EndQuorumEpochRequest.LeaderEndpoint{.{
            .name = "listener",
            .host = "broker-7",
            .port = 9092,
        }};
        const value = EndQuorumEpochRequest{
            .cluster_id = "cluster-a",
            .topics = &topics,
            .leader_endpoints = &leader_endpoints,
        };
        try expectGoldenRoundTrip(EndQuorumEpochRequest, value, 1, &[_]u8{
            0x0a, 'c',  'l',  'u',  's',  't',  'e',  'r',
            '-',  'a',  0x02, 0x08, 't',  'o',  'p',  'i',
            'c',  '-',  'a',  0x02, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x03,
            0x02, 0x00, 0x00, 0x00, 0x04, 0x10, 0x11, 0x12,
            0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a,
            0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x00, 0x00, 0x00,
            0x02, 0x09, 'l',  'i',  's',  't',  'e',  'n',
            'e',  'r',  0x09, 'b',  'r',  'o',  'k',  'e',
            'r',  '-',  '7',  0x23, 0x84, 0x00, 0x00,
        });
    }

    {
        const FetchSnapshotResponse = generated.fetch_snapshot_response.FetchSnapshotResponse;
        const PartitionSnapshot = FetchSnapshotResponse.TopicSnapshot.PartitionSnapshot;
        const partitions = [_]PartitionSnapshot{.{
            .index = 1,
            .error_code = 0,
            .snapshot_id = .{ .end_offset = 42, .epoch = 3 },
            .current_leader = .{ .leader_id = 7, .leader_epoch = 4 },
            .size = 128,
            .position = 16,
            .unaligned_records = &[_]u8{ 0xaa, 0xbb },
        }};
        const topics = [_]FetchSnapshotResponse.TopicSnapshot{.{
            .name = "snap-topic",
            .partitions = &partitions,
        }};
        const node_endpoints = [_]FetchSnapshotResponse.NodeEndpoint{.{
            .node_id = 7,
            .host = "n1",
            .port = 9092,
        }};
        const value = FetchSnapshotResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .topics = &topics,
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(FetchSnapshotResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x02, 0x0b,
            's',  'n',  'a',  'p',  '-',  't',  'o',  'p',
            'i',  'c',  0x02, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x2a, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x03, 0xaa,
            0xbb, 0x01, 0x00, 0x09, 0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x01, 0x00,
            0x0b, 0x02, 0x00, 0x00, 0x00, 0x07, 0x03, 'n',
            '1',  0x23, 0x84, 0x00,
        });
    }

    {
        const FetchSnapshotRequest = generated.fetch_snapshot_request.FetchSnapshotRequest;
        const replica_directory_id = [_]u8{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        };
        const partitions = [_]FetchSnapshotRequest.TopicSnapshot.PartitionSnapshot{.{
            .partition = 1,
            .current_leader_epoch = 3,
            .snapshot_id = .{ .end_offset = 42, .epoch = 4 },
            .position = 16,
            .replica_directory_id = replica_directory_id,
        }};
        const topics = [_]FetchSnapshotRequest.TopicSnapshot{.{
            .name = "snap-topic",
            .partitions = &partitions,
        }};
        const value = FetchSnapshotRequest{
            .cluster_id = "cluster-a",
            .replica_id = 2,
            .max_bytes = 1024,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(FetchSnapshotRequest, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x04, 0x00,
            0x02, 0x0b, 's',  'n',  'a',  'p',  '-',  't',
            'o',  'p',  'i',  'c',  0x02, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x10, 0x01, 0x00, 0x10, 0x00, 0x01, 0x02,
            0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
            0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x00, 0x01, 0x00,
            0x0a, 0x0a, 'c',  'l',  'u',  's',  't',  'e',
            'r',  '-',  'a',
        });
    }

    {
        const FetchRequest = generated.fetch_request.FetchRequest;
        const topic_id = [_]u8{
            0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        };
        const replica_directory_id = [_]u8{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        };
        const partitions = [_]FetchRequest.FetchTopic.FetchPartition{.{
            .partition = 0,
            .current_leader_epoch = 3,
            .fetch_offset = 42,
            .last_fetched_epoch = 2,
            .log_start_offset = 5,
            .partition_max_bytes = 4096,
            .replica_directory_id = replica_directory_id,
        }};
        const topics = [_]FetchRequest.FetchTopic{.{
            .topic_id = topic_id,
            .partitions = &partitions,
        }};
        const value = FetchRequest{
            .cluster_id = "cluster-a",
            .replica_state = .{ .replica_id = 7, .replica_epoch = 4 },
            .max_wait_ms = 500,
            .min_bytes = 1,
            .max_bytes = 1048576,
            .isolation_level = 1,
            .session_id = 123,
            .session_epoch = 1,
            .topics = &topics,
            .rack_id = "rack-a",
        };
        try expectGoldenRoundTrip(FetchRequest, value, 17, &[_]u8{
            0x00, 0x00, 0x01, 0xf4, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x10, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x7b, 0x00, 0x00, 0x00, 0x01, 0x02, 0x10, 0x11,
            0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
            0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x05, 0x00, 0x00, 0x10, 0x00, 0x01,
            0x00, 0x10, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
            0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x00, 0x01, 0x07, 'r',  'a',  'c',
            'k',  '-',  'a',  0x02, 0x00, 0x0a, 0x0a, 'c',
            'l',  'u',  's',  't',  'e',  'r',  '-',  'a',
            0x01, 0x0d, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
        });
    }

    {
        const BrokerHeartbeatRequest = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
        const offline_log_dirs = [_][16]u8{.{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        }};
        const value = BrokerHeartbeatRequest{
            .broker_id = 100,
            .broker_epoch = 7,
            .current_metadata_offset = 9,
            .want_fence = false,
            .want_shut_down = true,
            .offline_log_dirs = &offline_log_dirs,
        };
        try expectGoldenRoundTrip(BrokerHeartbeatRequest, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x64,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x09,
            0x00, 0x01, 0x01, 0x00,
            0x11, 0x02, 0x00, 0x01,
            0x02, 0x03, 0x04, 0x05,
            0x06, 0x07, 0x08, 0x09,
            0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        });
    }

    {
        const BrokerRegistrationRequest = generated.broker_registration_request.BrokerRegistrationRequest;
        const incarnation_id = [_]u8{
            0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        };
        const log_dirs = [_][16]u8{.{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        }};
        const listeners = [_]BrokerRegistrationRequest.Listener{.{
            .name = "PLAINTEXT",
            .host = "broker-1",
            .port = 9092,
            .security_protocol = 0,
        }};
        const features = [_]BrokerRegistrationRequest.Feature{.{
            .name = "metadata.version",
            .min_supported_version = 0,
            .max_supported_version = 4,
        }};
        const value = BrokerRegistrationRequest{
            .broker_id = 101,
            .cluster_id = "cluster-a",
            .incarnation_id = incarnation_id,
            .listeners = &listeners,
            .features = &features,
            .rack = "rack-a",
            .is_migrating_zk_broker = false,
            .log_dirs = &log_dirs,
        };
        try expectGoldenRoundTrip(BrokerRegistrationRequest, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x65,
            0x0a, 'c',  'l',  'u',
            's',  't',  'e',  'r',
            '-',  'a',  0x10, 0x11,
            0x12, 0x13, 0x14, 0x15,
            0x16, 0x17, 0x18, 0x19,
            0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f, 0x02, 0x0a,
            'P',  'L',  'A',  'I',
            'N',  'T',  'E',  'X',
            'T',  0x09, 'b',  'r',
            'o',  'k',  'e',  'r',
            '-',  '1',  0x23, 0x84,
            0x00, 0x00, 0x00, 0x02,
            0x11, 'm',  'e',  't',
            'a',  'd',  'a',  't',
            'a',  '.',  'v',  'e',
            'r',  's',  'i',  'o',
            'n',  0x00, 0x00, 0x00,
            0x04, 0x00, 0x07, 'r',
            'a',  'c',  'k',  '-',
            'a',  0x00, 0x02, 0x00,
            0x01, 0x02, 0x03, 0x04,
            0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c,
            0x0d, 0x0e, 0x0f, 0x00,
        });
    }

    {
        const BrokerRegistrationResponse = generated.broker_registration_response.BrokerRegistrationResponse;
        const value = BrokerRegistrationResponse{
            .throttle_time_ms = 12,
            .error_code = 0,
            .broker_epoch = 5,
        };
        try expectGoldenRoundTrip(BrokerRegistrationResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x0c,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x05, 0x00,
        });
    }

    {
        const BrokerHeartbeatResponse = generated.broker_heartbeat_response.BrokerHeartbeatResponse;
        const value = BrokerHeartbeatResponse{
            .throttle_time_ms = 5,
            .error_code = 0,
            .is_caught_up = true,
            .is_fenced = false,
            .should_shut_down = true,
        };
        try expectGoldenRoundTrip(BrokerHeartbeatResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x05,
            0x00, 0x00, 0x01, 0x00,
            0x01, 0x00,
        });
    }

    {
        const ControllerRegistrationRequest = generated.controller_registration_request.ControllerRegistrationRequest;
        const incarnation_id = [_]u8{
            0x20, 0x21, 0x22, 0x23,
            0x24, 0x25, 0x26, 0x27,
            0x28, 0x29, 0x2a, 0x2b,
            0x2c, 0x2d, 0x2e, 0x2f,
        };
        const listeners = [_]ControllerRegistrationRequest.Listener{.{
            .name = "CONTROLLER",
            .host = "controller-1",
            .port = 9093,
            .security_protocol = 0,
        }};
        const features = [_]ControllerRegistrationRequest.Feature{.{
            .name = "kraft.version",
            .min_supported_version = 0,
            .max_supported_version = 1,
        }};
        const value = ControllerRegistrationRequest{
            .controller_id = 3,
            .incarnation_id = incarnation_id,
            .zk_migration_ready = true,
            .listeners = &listeners,
            .features = &features,
        };
        try expectGoldenRoundTrip(ControllerRegistrationRequest, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x03,
            0x20, 0x21, 0x22, 0x23,
            0x24, 0x25, 0x26, 0x27,
            0x28, 0x29, 0x2a, 0x2b,
            0x2c, 0x2d, 0x2e, 0x2f,
            0x01, 0x02, 0x0b, 'C',
            'O',  'N',  'T',  'R',
            'O',  'L',  'L',  'E',
            'R',  0x0d, 'c',  'o',
            'n',  't',  'r',  'o',
            'l',  'l',  'e',  'r',
            '-',  '1',  0x23, 0x85,
            0x00, 0x00, 0x00, 0x02,
            0x0e, 'k',  'r',  'a',
            'f',  't',  '.',  'v',
            'e',  'r',  's',  'i',
            'o',  'n',  0x00, 0x00,
            0x00, 0x01, 0x00, 0x00,
        });
    }

    {
        const ControllerRegistrationResponse = generated.controller_registration_response.ControllerRegistrationResponse;
        const value = ControllerRegistrationResponse{
            .throttle_time_ms = 7,
            .error_code = 41,
            .error_message = "not controller",
        };
        try expectGoldenRoundTrip(ControllerRegistrationResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x29, 0x0f, 'n',
            'o',  't',  ' ',  'c',
            'o',  'n',  't',  'r',
            'o',  'l',  'l',  'e',
            'r',  0x00,
        });
    }

    {
        const AddRaftVoterRequest = generated.add_raft_voter_request.AddRaftVoterRequest;
        const voter_directory_id = [_]u8{
            0x30, 0x31, 0x32, 0x33,
            0x34, 0x35, 0x36, 0x37,
            0x38, 0x39, 0x3a, 0x3b,
            0x3c, 0x3d, 0x3e, 0x3f,
        };
        const listeners = [_]AddRaftVoterRequest.Listener{.{
            .name = "CONTROLLER",
            .host = "controller-4",
            .port = 19093,
        }};
        const value = AddRaftVoterRequest{
            .cluster_id = "cluster-a",
            .timeout_ms = 3000,
            .voter_id = 4,
            .voter_directory_id = voter_directory_id,
            .listeners = &listeners,
        };
        try expectGoldenRoundTrip(AddRaftVoterRequest, value, 0, &[_]u8{
            0x0a, 'c',  'l',  'u',
            's',  't',  'e',  'r',
            '-',  'a',  0x00, 0x00,
            0x0b, 0xb8, 0x00, 0x00,
            0x00, 0x04, 0x30, 0x31,
            0x32, 0x33, 0x34, 0x35,
            0x36, 0x37, 0x38, 0x39,
            0x3a, 0x3b, 0x3c, 0x3d,
            0x3e, 0x3f, 0x02, 0x0b,
            'C',  'O',  'N',  'T',
            'R',  'O',  'L',  'L',
            'E',  'R',  0x0d, 'c',
            'o',  'n',  't',  'r',
            'o',  'l',  'l',  'e',
            'r',  '-',  '4',  0x4a,
            0x95, 0x00, 0x00,
        });
    }

    {
        const AddRaftVoterResponse = generated.add_raft_voter_response.AddRaftVoterResponse;
        const value = AddRaftVoterResponse{
            .throttle_time_ms = 9,
            .error_code = 42,
            .error_message = "duplicate",
        };
        try expectGoldenRoundTrip(AddRaftVoterResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x09,
            0x00, 0x2a, 0x0a, 'd',
            'u',  'p',  'l',  'i',
            'c',  'a',  't',  'e',
            0x00,
        });
    }

    {
        const RemoveRaftVoterRequest = generated.remove_raft_voter_request.RemoveRaftVoterRequest;
        const voter_directory_id = [_]u8{
            0x30, 0x31, 0x32, 0x33,
            0x34, 0x35, 0x36, 0x37,
            0x38, 0x39, 0x3a, 0x3b,
            0x3c, 0x3d, 0x3e, 0x3f,
        };
        const value = RemoveRaftVoterRequest{
            .cluster_id = "cluster-a",
            .voter_id = 4,
            .voter_directory_id = voter_directory_id,
        };
        try expectGoldenRoundTrip(RemoveRaftVoterRequest, value, 0, &[_]u8{
            0x0a, 'c',  'l',  'u',
            's',  't',  'e',  'r',
            '-',  'a',  0x00, 0x00,
            0x00, 0x04, 0x30, 0x31,
            0x32, 0x33, 0x34, 0x35,
            0x36, 0x37, 0x38, 0x39,
            0x3a, 0x3b, 0x3c, 0x3d,
            0x3e, 0x3f, 0x00,
        });
    }

    {
        const RemoveRaftVoterResponse = generated.remove_raft_voter_response.RemoveRaftVoterResponse;
        const value = RemoveRaftVoterResponse{
            .throttle_time_ms = 10,
            .error_code = 42,
            .error_message = "missing",
        };
        try expectGoldenRoundTrip(RemoveRaftVoterResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x0a,
            0x00, 0x2a, 0x08, 'm',
            'i',  's',  's',  'i',
            'n',  'g',  0x00,
        });
    }

    {
        const UpdateRaftVoterRequest = generated.update_raft_voter_request.UpdateRaftVoterRequest;
        const voter_directory_id = [_]u8{
            0x30, 0x31, 0x32, 0x33,
            0x34, 0x35, 0x36, 0x37,
            0x38, 0x39, 0x3a, 0x3b,
            0x3c, 0x3d, 0x3e, 0x3f,
        };
        const listeners = [_]UpdateRaftVoterRequest.Listener{.{
            .name = "CONTROLLER",
            .host = "controller-4",
            .port = 19094,
        }};
        const value = UpdateRaftVoterRequest{
            .cluster_id = "cluster-a",
            .voter_id = 4,
            .voter_directory_id = voter_directory_id,
            .listeners = &listeners,
            .k_raft_version_feature = .{
                .min_supported_version = 1,
                .max_supported_version = 3,
            },
        };
        try expectGoldenRoundTrip(UpdateRaftVoterRequest, value, 0, &[_]u8{
            0x0a, 'c',  'l',  'u',
            's',  't',  'e',  'r',
            '-',  'a',  0x00, 0x00,
            0x00, 0x04, 0x30, 0x31,
            0x32, 0x33, 0x34, 0x35,
            0x36, 0x37, 0x38, 0x39,
            0x3a, 0x3b, 0x3c, 0x3d,
            0x3e, 0x3f, 0x02, 0x0b,
            'C',  'O',  'N',  'T',
            'R',  'O',  'L',  'L',
            'E',  'R',  0x0d, 'c',
            'o',  'n',  't',  'r',
            'o',  'l',  'l',  'e',
            'r',  '-',  '4',  0x4a,
            0x96, 0x00, 0x00, 0x01,
            0x00, 0x03, 0x00, 0x00,
        });
    }

    {
        const UpdateRaftVoterResponse = generated.update_raft_voter_response.UpdateRaftVoterResponse;
        const value = UpdateRaftVoterResponse{
            .throttle_time_ms = 11,
            .error_code = 21,
        };
        try expectGoldenRoundTrip(UpdateRaftVoterResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x0b,
            0x00, 0x15, 0x00,
        });
    }

    {
        const ApiVersionsResponse = generated.api_versions_response.ApiVersionsResponse;
        const api_keys = [_]ApiVersionsResponse.ApiVersion{
            .{ .api_key = 0, .min_version = 0, .max_version = 11 },
            .{ .api_key = 3, .min_version = 0, .max_version = 12 },
        };
        const supported_features = [_]ApiVersionsResponse.SupportedFeatureKey{.{
            .name = "metadata.version",
            .min_version = 0,
            .max_version = 4,
        }};
        const finalized_features = [_]ApiVersionsResponse.FinalizedFeatureKey{.{
            .name = "metadata.version",
            .max_version_level = 4,
            .min_version_level = 1,
        }};
        const unknown_tag_data = [_]u8{ 0xaa, 0xbb };
        const unknown_tags = [_]ser.TaggedField{.{
            .tag = 9,
            .data = &unknown_tag_data,
        }};
        const value = ApiVersionsResponse{
            .error_code = 0,
            .api_keys = &api_keys,
            .throttle_time_ms = 42,
            .supported_features = &supported_features,
            .finalized_features_epoch = 42,
            .finalized_features = &finalized_features,
            .zk_migration_ready = true,
            .tagged_fields = &unknown_tags,
        };
        try expectGoldenRoundTrip(ApiVersionsResponse, value, 3, &[_]u8{
            0x00, 0x00,
            0x03, 0x00,
            0x00, 0x00,
            0x00, 0x00,
            0x0b, 0x00,
            0x00, 0x03,
            0x00, 0x00,
            0x00, 0x0c,
            0x00, 0x00,
            0x00, 0x00,
            0x2a, 0x05,
            0x00, 0x17,
            0x02, 0x11,
            'm',  'e',
            't',  'a',
            'd',  'a',
            't',  'a',
            '.',  'v',
            'e',  'r',
            's',  'i',
            'o',  'n',
            0x00, 0x00,
            0x00, 0x04,
            0x00, 0x01,
            0x08, 0x00,
            0x00, 0x00,
            0x00, 0x00,
            0x00, 0x00,
            0x2a, 0x02,
            0x17, 0x02,
            0x11, 'm',
            'e',  't',
            'a',  'd',
            'a',  't',
            'a',  '.',
            'v',  'e',
            'r',  's',
            'i',  'o',
            'n',  0x00,
            0x04, 0x00,
            0x01, 0x00,
            0x03, 0x01,
            0x01, 0x09,
            0x02, 0xaa,
            0xbb,
        });
    }

    {
        const FindCoordinatorRequest = generated.find_coordinator_request.FindCoordinatorRequest;
        const legacy_value = FindCoordinatorRequest{
            .key = "group-a",
            .key_type = 0,
        };
        try expectGoldenRoundTrip(FindCoordinatorRequest, legacy_value, 2, &[_]u8{
            0x00, 0x07, 'g', 'r',
            'o',  'u',  'p', '-',
            'a',  0x00,
        });

        const coordinator_keys = [_]?[]const u8{ "txn-a", "txn-b" };
        const flexible_value = FindCoordinatorRequest{
            .key_type = 1,
            .coordinator_keys = &coordinator_keys,
        };
        try expectGoldenRoundTrip(FindCoordinatorRequest, flexible_value, 4, &[_]u8{
            0x01, 0x03, 0x06, 't',
            'x',  'n',  '-',  'a',
            0x06, 't',  'x',  'n',
            '-',  'b',  0x00,
        });
    }

    {
        const FindCoordinatorResponse = generated.find_coordinator_response.FindCoordinatorResponse;
        const legacy_value = FindCoordinatorResponse{
            .throttle_time_ms = 5,
            .error_code = 0,
            .error_message = null,
            .node_id = 2,
            .host = "broker-2",
            .port = 19092,
        };
        try expectGoldenRoundTrip(FindCoordinatorResponse, legacy_value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x05,
            0x00, 0x00, 0xff, 0xff,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x08, 'b',  'r',
            'o',  'k',  'e',  'r',
            '-',  '2',  0x00, 0x00,
            0x4a, 0x94,
        });

        const coordinators = [_]FindCoordinatorResponse.Coordinator{
            .{
                .key = "group-a",
                .node_id = 2,
                .host = "broker-2",
                .port = 19092,
                .error_code = 0,
                .error_message = null,
            },
            .{
                .key = "txn-a",
                .node_id = -1,
                .host = null,
                .port = -1,
                .error_code = 15,
                .error_message = "missing",
            },
        };
        const flexible_value = FindCoordinatorResponse{
            .throttle_time_ms = 5,
            .coordinators = &coordinators,
        };
        try expectGoldenRoundTrip(FindCoordinatorResponse, flexible_value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x05,
            0x03, 0x08, 'g',  'r',
            'o',  'u',  'p',  '-',
            'a',  0x00, 0x00, 0x00,
            0x02, 0x09, 'b',  'r',
            'o',  'k',  'e',  'r',
            '-',  '2',  0x00, 0x00,
            0x4a, 0x94, 0x00, 0x00,
            0x00, 0x00, 0x06, 't',
            'x',  'n',  '-',  'a',
            0xff, 0xff, 0xff, 0xff,
            0x00, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x0f, 0x08,
            'm',  'i',  's',  's',
            'i',  'n',  'g',  0x00,
            0x00,
        });
    }

    {
        const ListOffsetsRequest = generated.list_offsets_request.ListOffsetsRequest;
        const legacy_partitions = [_]ListOffsetsRequest.ListOffsetsTopic.ListOffsetsPartition{.{
            .partition_index = 0,
            .timestamp = -2,
            .max_num_offsets = 1,
        }};
        const legacy_topics = [_]ListOffsetsRequest.ListOffsetsTopic{.{
            .name = "topic-a",
            .partitions = &legacy_partitions,
        }};
        const legacy_value = ListOffsetsRequest{
            .replica_id = -1,
            .topics = &legacy_topics,
        };
        try expectGoldenRoundTrip(ListOffsetsRequest, legacy_value, 0, &[_]u8{
            0xff, 0xff, 0xff, 0xff,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x07, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x00, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xfe, 0x00, 0x00, 0x00,
            0x01,
        });

        const flexible_partitions = [_]ListOffsetsRequest.ListOffsetsTopic.ListOffsetsPartition{.{
            .partition_index = 0,
            .current_leader_epoch = 9,
            .timestamp = -1,
        }};
        const flexible_topics = [_]ListOffsetsRequest.ListOffsetsTopic{.{
            .name = "topic-a",
            .partitions = &flexible_partitions,
        }};
        const flexible_value = ListOffsetsRequest{
            .replica_id = -1,
            .isolation_level = 1,
            .topics = &flexible_topics,
        };
        try expectGoldenRoundTrip(ListOffsetsRequest, flexible_value, 7, &[_]u8{
            0xff, 0xff, 0xff, 0xff,
            0x01, 0x02, 0x08, 't',
            'o',  'p',  'i',  'c',
            '-',  'a',  0x02, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x09, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0x00,
            0x00, 0x00,
        });
    }

    {
        const ListOffsetsResponse = generated.list_offsets_response.ListOffsetsResponse;
        const legacy_partitions = [_]ListOffsetsResponse.ListOffsetsTopicResponse.ListOffsetsPartitionResponse{.{
            .partition_index = 0,
            .error_code = 0,
            .old_style_offsets = &[_]i64{ 0, 42 },
        }};
        const legacy_topics = [_]ListOffsetsResponse.ListOffsetsTopicResponse{.{
            .name = "topic-a",
            .partitions = &legacy_partitions,
        }};
        const legacy_value = ListOffsetsResponse{
            .topics = &legacy_topics,
        };
        try expectGoldenRoundTrip(ListOffsetsResponse, legacy_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x07, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x2a,
        });

        const flexible_partitions = [_]ListOffsetsResponse.ListOffsetsTopicResponse.ListOffsetsPartitionResponse{.{
            .partition_index = 0,
            .error_code = 0,
            .timestamp = 1_700_000_000_000,
            .offset = 42,
            .leader_epoch = 9,
        }};
        const flexible_topics = [_]ListOffsetsResponse.ListOffsetsTopicResponse{.{
            .name = "topic-a",
            .partitions = &flexible_partitions,
        }};
        const flexible_value = ListOffsetsResponse{
            .throttle_time_ms = 5,
            .topics = &flexible_topics,
        };
        try expectGoldenRoundTrip(ListOffsetsResponse, flexible_value, 7, &[_]u8{
            0x00, 0x00, 0x00, 0x05,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x8b,
            0xcf, 0xe5, 0x68, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x2a,
            0x00, 0x00, 0x00, 0x09,
            0x00, 0x00, 0x00,
        });
    }

    {
        const OffsetForLeaderEpochRequest = generated.offset_for_leader_epoch_request.OffsetForLeaderEpochRequest;
        const partitions = [_]OffsetForLeaderEpochRequest.OffsetForLeaderTopic.OffsetForLeaderPartition{.{
            .partition = 0,
            .current_leader_epoch = 3,
            .leader_epoch = 2,
        }};
        const topics = [_]OffsetForLeaderEpochRequest.OffsetForLeaderTopic{.{
            .topic = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetForLeaderEpochRequest{
            .replica_id = -1,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(OffsetForLeaderEpochRequest, value, 4, &[_]u8{
            0xff, 0xff, 0xff, 0xff,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00,
            0x00,
        });
    }

    {
        const OffsetForLeaderEpochResponse = generated.offset_for_leader_epoch_response.OffsetForLeaderEpochResponse;
        const partitions = [_]OffsetForLeaderEpochResponse.OffsetForLeaderTopicResult.EpochEndOffset{.{
            .partition = 0,
            .leader_epoch = 2,
            .end_offset = 42,
        }};
        const topics = [_]OffsetForLeaderEpochResponse.OffsetForLeaderTopicResult{.{
            .topic = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetForLeaderEpochResponse{
            .throttle_time_ms = 9,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(OffsetForLeaderEpochResponse, value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x09,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x2a,
            0x00, 0x00, 0x00,
        });
    }

    {
        const DeleteRecordsRequest = generated.delete_records_request.DeleteRecordsRequest;
        const partitions = [_]DeleteRecordsRequest.DeleteRecordsTopic.DeleteRecordsPartition{.{
            .partition_index = 0,
            .offset = 42,
        }};
        const topics = [_]DeleteRecordsRequest.DeleteRecordsTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = DeleteRecordsRequest{
            .topics = &topics,
            .timeout_ms = 30000,
        };
        try expectGoldenRoundTrip(DeleteRecordsRequest, value, 2, &[_]u8{
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x2a, 0x00, 0x00,
            0x00, 0x00, 0x75, 0x30,
            0x00,
        });
    }

    {
        const DeleteRecordsResponse = generated.delete_records_response.DeleteRecordsResponse;
        const partitions = [_]DeleteRecordsResponse.DeleteRecordsTopicResult.DeleteRecordsPartitionResult{.{
            .partition_index = 0,
            .low_watermark = 42,
        }};
        const topics = [_]DeleteRecordsResponse.DeleteRecordsTopicResult{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = DeleteRecordsResponse{
            .throttle_time_ms = 8,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(DeleteRecordsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x08,
            0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x2a, 0x00, 0x00,
            0x00, 0x00, 0x00,
        });
    }

    {
        const JoinGroupRequest = generated.join_group_request.JoinGroupRequest;
        const protocols = [_]JoinGroupRequest.JoinGroupRequestProtocol{
            .{
                .name = "range",
                .metadata = &[_]u8{ 0x01, 0x02, 0x03 },
            },
            .{
                .name = "sticky",
                .metadata = &[_]u8{0xaa},
            },
        };
        const value = JoinGroupRequest{
            .group_id = "g1",
            .session_timeout_ms = 45_000,
            .rebalance_timeout_ms = 60_000,
            .member_id = "member-A",
            .group_instance_id = "inst-A",
            .protocol_type = "consumer",
            .protocols = &protocols,
            .reason = "rebalance",
        };
        try expectGoldenRoundTrip(JoinGroupRequest, value, 5, &[_]u8{
            0x00, 0x02, 'g',  '1',
            0x00, 0x00, 0xaf, 0xc8,
            0x00, 0x00, 0xea, 0x60,
            0x00, 0x08, 'm',  'e',
            'm',  'b',  'e',  'r',
            '-',  'A',  0x00, 0x06,
            'i',  'n',  's',  't',
            '-',  'A',  0x00, 0x08,
            'c',  'o',  'n',  's',
            'u',  'm',  'e',  'r',
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x05, 'r',  'a',
            'n',  'g',  'e',  0x00,
            0x00, 0x00, 0x03, 0x01,
            0x02, 0x03, 0x00, 0x06,
            's',  't',  'i',  'c',
            'k',  'y',  0x00, 0x00,
            0x00, 0x01, 0xaa,
        });
        try expectGoldenRoundTrip(JoinGroupRequest, value, 9, &[_]u8{
            0x03, 'g',  '1',
            0x00, 0x00, 0xaf,
            0xc8, 0x00, 0x00,
            0xea, 0x60, 0x09,
            'm',  'e',  'm',
            'b',  'e',  'r',
            '-',  'A',  0x07,
            'i',  'n',  's',
            't',  '-',  'A',
            0x09, 'c',  'o',
            'n',  's',  'u',
            'm',  'e',  'r',
            0x03, 0x06, 'r',
            'a',  'n',  'g',
            'e',  0x04, 0x01,
            0x02, 0x03, 0x00,
            0x07, 's',  't',
            'i',  'c',  'k',
            'y',  0x02, 0xaa,
            0x00, 0x0a, 'r',
            'e',  'b',  'a',
            'l',  'a',  'n',
            'c',  'e',  0x00,
        });
    }

    {
        const JoinGroupResponse = generated.join_group_response.JoinGroupResponse;
        const members = [_]JoinGroupResponse.JoinGroupResponseMember{
            .{
                .member_id = "member-A",
                .group_instance_id = "inst-A",
                .metadata = &[_]u8{ 0x10, 0x11 },
            },
            .{
                .member_id = "member-B",
                .group_instance_id = null,
                .metadata = &[_]u8{0x20},
            },
        };
        const value = JoinGroupResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .generation_id = 3,
            .protocol_type = "consumer",
            .protocol_name = "range",
            .leader = "member-A",
            .skip_assignment = true,
            .member_id = "member-B",
            .members = &members,
        };
        try expectGoldenRoundTrip(JoinGroupResponse, value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x05,
            'r',  'a',  'n',  'g',
            'e',  0x00, 0x08, 'm',
            'e',  'm',  'b',  'e',
            'r',  '-',  'A',  0x00,
            0x08, 'm',  'e',  'm',
            'b',  'e',  'r',  '-',
            'B',  0x00, 0x00, 0x00,
            0x02, 0x00, 0x08, 'm',
            'e',  'm',  'b',  'e',
            'r',  '-',  'A',  0x00,
            0x06, 'i',  'n',  's',
            't',  '-',  'A',  0x00,
            0x00, 0x00, 0x02, 0x10,
            0x11, 0x00, 0x08, 'm',
            'e',  'm',  'b',  'e',
            'r',  '-',  'B',  0xff,
            0xff, 0x00, 0x00, 0x00,
            0x01, 0x20,
        });
        try expectGoldenRoundTrip(JoinGroupResponse, value, 9, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x09, 'c',
            'o',  'n',  's',  'u',
            'm',  'e',  'r',  0x06,
            'r',  'a',  'n',  'g',
            'e',  0x09, 'm',  'e',
            'm',  'b',  'e',  'r',
            '-',  'A',  0x01, 0x09,
            'm',  'e',  'm',  'b',
            'e',  'r',  '-',  'B',
            0x03, 0x09, 'm',  'e',
            'm',  'b',  'e',  'r',
            '-',  'A',  0x07, 'i',
            'n',  's',  't',  '-',
            'A',  0x03, 0x10, 0x11,
            0x00, 0x09, 'm',  'e',
            'm',  'b',  'e',  'r',
            '-',  'B',  0x00, 0x02,
            0x20, 0x00, 0x00,
        });
    }

    {
        const SyncGroupRequest = generated.sync_group_request.SyncGroupRequest;
        const assignments = [_]SyncGroupRequest.SyncGroupRequestAssignment{
            .{
                .member_id = "member-A",
                .assignment = &[_]u8{ 0x01, 0x02 },
            },
            .{
                .member_id = "member-B",
                .assignment = null,
            },
        };
        const value = SyncGroupRequest{
            .group_id = "g1",
            .generation_id = 3,
            .member_id = "member-A",
            .group_instance_id = "inst-A",
            .protocol_type = "consumer",
            .protocol_name = "range",
            .assignments = &assignments,
        };
        try expectGoldenRoundTrip(SyncGroupRequest, value, 3, &[_]u8{
            0x00, 0x02, 'g',  '1',
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x08, 'm',  'e',
            'm',  'b',  'e',  'r',
            '-',  'A',  0x00, 0x06,
            'i',  'n',  's',  't',
            '-',  'A',  0x00, 0x00,
            0x00, 0x02, 0x00, 0x08,
            'm',  'e',  'm',  'b',
            'e',  'r',  '-',  'A',
            0x00, 0x00, 0x00, 0x02,
            0x01, 0x02, 0x00, 0x08,
            'm',  'e',  'm',  'b',
            'e',  'r',  '-',  'B',
            0xff, 0xff, 0xff, 0xff,
        });
        try expectGoldenRoundTrip(SyncGroupRequest, value, 5, &[_]u8{
            0x03, 'g',  '1',
            0x00, 0x00, 0x00,
            0x03, 0x09, 'm',
            'e',  'm',  'b',
            'e',  'r',  '-',
            'A',  0x07, 'i',
            'n',  's',  't',
            '-',  'A',  0x09,
            'c',  'o',  'n',
            's',  'u',  'm',
            'e',  'r',  0x06,
            'r',  'a',  'n',
            'g',  'e',  0x03,
            0x09, 'm',  'e',
            'm',  'b',  'e',
            'r',  '-',  'A',
            0x03, 0x01, 0x02,
            0x00, 0x09, 'm',
            'e',  'm',  'b',
            'e',  'r',  '-',
            'B',  0x00, 0x00,
            0x00,
        });
    }

    {
        const SyncGroupResponse = generated.sync_group_response.SyncGroupResponse;
        const value = SyncGroupResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .protocol_type = "consumer",
            .protocol_name = "range",
            .assignment = &[_]u8{ 0xab, 0xcd },
        };
        try expectGoldenRoundTrip(SyncGroupResponse, value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x02, 0xab, 0xcd,
        });
        try expectGoldenRoundTrip(SyncGroupResponse, value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x09, 'c',
            'o',  'n',  's',  'u',
            'm',  'e',  'r',  0x06,
            'r',  'a',  'n',  'g',
            'e',  0x03, 0xab, 0xcd,
            0x00,
        });
    }

    {
        const HeartbeatRequest = generated.heartbeat_request.HeartbeatRequest;
        const value = HeartbeatRequest{
            .group_id = "g1",
            .generation_id = 3,
            .member_id = "member-A",
            .group_instance_id = "inst-A",
        };
        try expectGoldenRoundTrip(HeartbeatRequest, value, 3, &[_]u8{
            0x00, 0x02, 'g',  '1',
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x08, 'm',  'e',
            'm',  'b',  'e',  'r',
            '-',  'A',  0x00, 0x06,
            'i',  'n',  's',  't',
            '-',  'A',
        });
        try expectGoldenRoundTrip(HeartbeatRequest, value, 4, &[_]u8{
            0x03, 'g',  '1',
            0x00, 0x00, 0x00,
            0x03, 0x09, 'm',
            'e',  'm',  'b',
            'e',  'r',  '-',
            'A',  0x07, 'i',
            'n',  's',  't',
            '-',  'A',  0x00,
        });
    }

    {
        const HeartbeatResponse = generated.heartbeat_response.HeartbeatResponse;
        const value = HeartbeatResponse{
            .throttle_time_ms = 7,
            .error_code = 25,
        };
        try expectGoldenRoundTrip(HeartbeatResponse, value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x19,
        });
        try expectGoldenRoundTrip(HeartbeatResponse, value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x19, 0x00,
        });
    }

    {
        const LeaveGroupRequest = generated.leave_group_request.LeaveGroupRequest;
        const legacy_value = LeaveGroupRequest{
            .group_id = "g1",
            .member_id = "member-A",
        };
        try expectGoldenRoundTrip(LeaveGroupRequest, legacy_value, 2, &[_]u8{
            0x00, 0x02, 'g', '1',
            0x00, 0x08, 'm', 'e',
            'm',  'b',  'e', 'r',
            '-',  'A',
        });

        const members = [_]LeaveGroupRequest.MemberIdentity{
            .{
                .member_id = "member-A",
                .group_instance_id = "inst-A",
                .reason = "shutdown",
            },
            .{
                .member_id = "member-B",
                .group_instance_id = null,
                .reason = "rebalance",
            },
        };
        const flexible_value = LeaveGroupRequest{
            .group_id = "g1",
            .members = &members,
        };
        try expectGoldenRoundTrip(LeaveGroupRequest, flexible_value, 5, &[_]u8{
            0x03, 'g',  '1',
            0x03, 0x09, 'm',
            'e',  'm',  'b',
            'e',  'r',  '-',
            'A',  0x07, 'i',
            'n',  's',  't',
            '-',  'A',  0x09,
            's',  'h',  'u',
            't',  'd',  'o',
            'w',  'n',  0x00,
            0x09, 'm',  'e',
            'm',  'b',  'e',
            'r',  '-',  'B',
            0x00, 0x0a, 'r',
            'e',  'b',  'a',
            'l',  'a',  'n',
            'c',  'e',  0x00,
            0x00,
        });
    }

    {
        const LeaveGroupResponse = generated.leave_group_response.LeaveGroupResponse;
        const legacy_value = LeaveGroupResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
        };
        try expectGoldenRoundTrip(LeaveGroupResponse, legacy_value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00,
        });

        const members = [_]LeaveGroupResponse.MemberResponse{
            .{
                .member_id = "member-A",
                .group_instance_id = "inst-A",
                .error_code = 0,
            },
            .{
                .member_id = "member-B",
                .group_instance_id = null,
                .error_code = 25,
            },
        };
        const flexible_value = LeaveGroupResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .members = &members,
        };
        try expectGoldenRoundTrip(LeaveGroupResponse, flexible_value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x03, 0x09,
            'm',  'e',  'm',  'b',
            'e',  'r',  '-',  'A',
            0x07, 'i',  'n',  's',
            't',  '-',  'A',  0x00,
            0x00, 0x00, 0x09, 'm',
            'e',  'm',  'b',  'e',
            'r',  '-',  'B',  0x00,
            0x00, 0x19, 0x00, 0x00,
        });
    }

    {
        const ListGroupsRequest = generated.list_groups_request.ListGroupsRequest;
        const states = [_]?[]const u8{"Stable"};
        const types = [_]?[]const u8{ "consumer", "share" };
        const value = ListGroupsRequest{
            .states_filter = &states,
            .types_filter = &types,
        };
        try expectGoldenRoundTrip(ListGroupsRequest, value, 5, &[_]u8{
            0x02,
            0x07,
            'S',
            't',
            'a',
            'b',
            'l',
            'e',
            0x03,
            0x09,
            'c',
            'o',
            'n',
            's',
            'u',
            'm',
            'e',
            'r',
            0x06,
            's',
            'h',
            'a',
            'r',
            'e',
            0x00,
        });
    }

    {
        const ListGroupsResponse = generated.list_groups_response.ListGroupsResponse;
        const groups = [_]ListGroupsResponse.ListedGroup{
            .{
                .group_id = "g1",
                .protocol_type = "consumer",
                .group_state = "Stable",
                .group_type = "classic",
            },
            .{
                .group_id = "share-g",
                .protocol_type = "share",
                .group_state = "Empty",
                .group_type = "share",
            },
        };
        const value = ListGroupsResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .groups = &groups,
        };
        try expectGoldenRoundTrip(ListGroupsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x02,
            'g',  '1',  0x00, 0x08,
            'c',  'o',  'n',  's',
            'u',  'm',  'e',  'r',
            0x00, 0x07, 's',  'h',
            'a',  'r',  'e',  '-',
            'g',  0x00, 0x05, 's',
            'h',  'a',  'r',  'e',
        });
        try expectGoldenRoundTrip(ListGroupsResponse, value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x03, 0x03,
            'g',  '1',  0x09, 'c',
            'o',  'n',  's',  'u',
            'm',  'e',  'r',  0x07,
            'S',  't',  'a',  'b',
            'l',  'e',  0x08, 'c',
            'l',  'a',  's',  's',
            'i',  'c',  0x00, 0x08,
            's',  'h',  'a',  'r',
            'e',  '-',  'g',  0x06,
            's',  'h',  'a',  'r',
            'e',  0x06, 'E',  'm',
            'p',  't',  'y',  0x06,
            's',  'h',  'a',  'r',
            'e',  0x00, 0x00,
        });
    }

    {
        const DescribeGroupsRequest = generated.describe_groups_request.DescribeGroupsRequest;
        const groups = [_]?[]const u8{ "g1", "share-g" };
        const value = DescribeGroupsRequest{
            .groups = &groups,
            .include_authorized_operations = true,
        };
        try expectGoldenRoundTrip(DescribeGroupsRequest, value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x02, 'g',  '1',
            0x00, 0x07, 's',  'h',
            'a',  'r',  'e',  '-',
            'g',  0x01,
        });
        try expectGoldenRoundTrip(DescribeGroupsRequest, value, 5, &[_]u8{
            0x03,
            0x03,
            'g',
            '1',
            0x08,
            's',
            'h',
            'a',
            'r',
            'e',
            '-',
            'g',
            0x01,
            0x00,
        });
    }

    {
        const DescribeGroupsResponse = generated.describe_groups_response.DescribeGroupsResponse;
        const members = [_]DescribeGroupsResponse.DescribedGroup.DescribedGroupMember{.{
            .member_id = "member-A",
            .group_instance_id = "inst-A",
            .client_id = "client-1",
            .client_host = "/127.0.0.1",
            .member_metadata = &[_]u8{ 0x01, 0x02 },
            .member_assignment = &[_]u8{0x0a},
        }};
        const groups = [_]DescribeGroupsResponse.DescribedGroup{.{
            .error_code = 0,
            .group_id = "g1",
            .group_state = "Stable",
            .protocol_type = "consumer",
            .protocol_data = "range",
            .members = &members,
            .authorized_operations = 9,
        }};
        const value = DescribeGroupsResponse{
            .throttle_time_ms = 7,
            .groups = &groups,
        };
        try expectGoldenRoundTrip(DescribeGroupsResponse, value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x02,
            'g',  '1',  0x00, 0x06,
            'S',  't',  'a',  'b',
            'l',  'e',  0x00, 0x08,
            'c',  'o',  'n',  's',
            'u',  'm',  'e',  'r',
            0x00, 0x05, 'r',  'a',
            'n',  'g',  'e',  0x00,
            0x00, 0x00, 0x01, 0x00,
            0x08, 'm',  'e',  'm',
            'b',  'e',  'r',  '-',
            'A',  0x00, 0x06, 'i',
            'n',  's',  't',  '-',
            'A',  0x00, 0x08, 'c',
            'l',  'i',  'e',  'n',
            't',  '-',  '1',  0x00,
            0x0a, '/',  '1',  '2',
            '7',  '.',  '0',  '.',
            '0',  '.',  '1',  0x00,
            0x00, 0x00, 0x02, 0x01,
            0x02, 0x00, 0x00, 0x00,
            0x01, 0x0a, 0x00, 0x00,
            0x00, 0x09,
        });
        try expectGoldenRoundTrip(DescribeGroupsResponse, value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x02, 0x00, 0x00, 0x03,
            'g',  '1',  0x07, 'S',
            't',  'a',  'b',  'l',
            'e',  0x09, 'c',  'o',
            'n',  's',  'u',  'm',
            'e',  'r',  0x06, 'r',
            'a',  'n',  'g',  'e',
            0x02, 0x09, 'm',  'e',
            'm',  'b',  'e',  'r',
            '-',  'A',  0x07, 'i',
            'n',  's',  't',  '-',
            'A',  0x09, 'c',  'l',
            'i',  'e',  'n',  't',
            '-',  '1',  0x0b, '/',
            '1',  '2',  '7',  '.',
            '0',  '.',  '0',  '.',
            '1',  0x03, 0x01, 0x02,
            0x02, 0x0a, 0x00, 0x00,
            0x00, 0x00, 0x09, 0x00,
            0x00,
        });
    }

    {
        const OffsetFetchRequest = generated.offset_fetch_request.OffsetFetchRequest;
        const value = OffsetFetchRequest{
            .group_id = "group-a",
            .topics = null,
        };
        try expectGoldenRoundTrip(OffsetFetchRequest, value, 2, &[_]u8{
            0x00, 0x07, 'g',  'r',  'o',  'u', 'p', '-',
            'a',  0xff, 0xff, 0xff, 0xff,
        });
    }

    {
        const OffsetFetchRequest = generated.offset_fetch_request.OffsetFetchRequest;
        const groups = [_]OffsetFetchRequest.OffsetFetchRequestGroup{.{
            .group_id = "group-a",
            .topics = null,
        }};
        const value = OffsetFetchRequest{
            .groups = &groups,
            .require_stable = true,
        };
        try expectGoldenRoundTrip(OffsetFetchRequest, value, 8, &[_]u8{
            0x02, 0x08, 'g',  'r',  'o',  'u', 'p', '-',
            'a',  0x00, 0x00, 0x01, 0x00,
        });
    }

    {
        const ConsumerGroupDescribeRequest = generated.consumer_group_describe_request.ConsumerGroupDescribeRequest;
        const group_ids = [_]?[]const u8{ "group-a", "group-b" };
        const value = ConsumerGroupDescribeRequest{
            .group_ids = &group_ids,
            .include_authorized_operations = true,
        };
        try expectGoldenRoundTrip(ConsumerGroupDescribeRequest, value, 0, &[_]u8{
            0x03, 0x08, 'g',  'r', 'o', 'u', 'p', '-',
            'a',  0x08, 'g',  'r', 'o', 'u', 'p', '-',
            'b',  0x01, 0x00,
        });
    }

    {
        const ConsumerGroupDescribeResponse = generated.consumer_group_describe_response.ConsumerGroupDescribeResponse;
        const Assignment = generated.consumer_group_describe_response.Assignment;
        const TopicPartitions = generated.consumer_group_describe_response.TopicPartitions;
        const partitions = [_]i32{ 0, 2 };
        const assigned_topics = [_]TopicPartitions{.{
            .topic_id = .{
                0x10, 0x11, 0x12, 0x13,
                0x14, 0x15, 0x16, 0x17,
                0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            },
            .topic_name = "topic-a",
            .partitions = &partitions,
        }};
        const subscribed_topics = [_]?[]const u8{ "topic-a", "topic-b" };
        const members = [_]ConsumerGroupDescribeResponse.DescribedGroup.Member{.{
            .member_id = "member-a",
            .instance_id = "instance-a",
            .rack_id = "rack-a",
            .member_epoch = 5,
            .client_id = "client-a",
            .client_host = "/127.0.0.1",
            .subscribed_topic_names = &subscribed_topics,
            .assignment = Assignment{ .topic_partitions = &assigned_topics },
            .target_assignment = Assignment{},
        }};
        const groups = [_]ConsumerGroupDescribeResponse.DescribedGroup{.{
            .group_id = "group-a",
            .group_state = "Stable",
            .group_epoch = 3,
            .assignment_epoch = 4,
            .assignor_name = "range",
            .members = &members,
            .authorized_operations = 5,
        }};
        const value = ConsumerGroupDescribeResponse{
            .throttle_time_ms = 5,
            .groups = &groups,
        };
        try expectGoldenRoundTrip(ConsumerGroupDescribeResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x02, 0x00, 0x00, 0x00,
            0x08, 'g',  'r',  'o',  'u',  'p',  '-',  'a',
            0x07, 'S',  't',  'a',  'b',  'l',  'e',  0x00,
            0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x06,
            'r',  'a',  'n',  'g',  'e',  0x02, 0x09, 'm',
            'e',  'm',  'b',  'e',  'r',  '-',  'a',  0x0b,
            'i',  'n',  's',  't',  'a',  'n',  'c',  'e',
            '-',  'a',  0x07, 'r',  'a',  'c',  'k',  '-',
            'a',  0x00, 0x00, 0x00, 0x05, 0x09, 'c',  'l',
            'i',  'e',  'n',  't',  '-',  'a',  0x0b, '/',
            '1',  '2',  '7',  '.',  '0',  '.',  '0',  '.',
            '1',  0x03, 0x08, 't',  'o',  'p',  'i',  'c',
            '-',  'a',  0x08, 't',  'o',  'p',  'i',  'c',
            '-',  'b',  0x00, 0x02, 0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f, 0x08, 't',  'o',  'p',
            'i',  'c',  '-',  'a',  0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
        });
    }

    {
        const OffsetCommitRequest = generated.offset_commit_request.OffsetCommitRequest;
        const partitions = [_]OffsetCommitRequest.OffsetCommitRequestTopic.OffsetCommitRequestPartition{.{
            .partition_index = 2,
            .committed_offset = 42,
            .commit_timestamp = 123456789,
            .committed_metadata = "meta-a",
        }};
        const topics = [_]OffsetCommitRequest.OffsetCommitRequestTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetCommitRequest{
            .group_id = "group-a",
            .generation_id_or_member_epoch = 3,
            .member_id = "member-a",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(OffsetCommitRequest, value, 1, &[_]u8{
            0x00, 0x07, 'g',  'r',  'o',  'u',  'p',  '-',
            'a',  0x00, 0x00, 0x00, 0x03, 0x00, 0x08, 'm',
            'e',  'm',  'b',  'e',  'r',  '-',  'a',  0x00,
            0x00, 0x00, 0x01, 0x00, 0x07, 't',  'o',  'p',
            'i',  'c',  '-',  'a',  0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00,
            0x07, 0x5b, 0xcd, 0x15, 0x00, 0x06, 'm',  'e',
            't',  'a',  '-',  'a',
        });
    }

    {
        const OffsetCommitRequest = generated.offset_commit_request.OffsetCommitRequest;
        const partitions = [_]OffsetCommitRequest.OffsetCommitRequestTopic.OffsetCommitRequestPartition{.{
            .partition_index = 2,
            .committed_offset = 42,
            .committed_leader_epoch = 9,
            .committed_metadata = "meta-a",
        }};
        const topics = [_]OffsetCommitRequest.OffsetCommitRequestTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetCommitRequest{
            .group_id = "group-a",
            .generation_id_or_member_epoch = 7,
            .member_id = "member-a",
            .group_instance_id = "instance-a",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(OffsetCommitRequest, value, 8, &[_]u8{
            0x08, 'g',  'r',  'o',  'u',  'p',  '-',  'a',
            0x00, 0x00, 0x00, 0x07, 0x09, 'm',  'e',  'm',
            'b',  'e',  'r',  '-',  'a',  0x0b, 'i',  'n',
            's',  't',  'a',  'n',  'c',  'e',  '-',  'a',
            0x02, 0x08, 't',  'o',  'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00,
            0x00, 0x09, 0x07, 'm',  'e',  't',  'a',  '-',
            'a',  0x00, 0x00, 0x00,
        });
    }

    {
        const OffsetCommitResponse = generated.offset_commit_response.OffsetCommitResponse;
        const partitions = [_]OffsetCommitResponse.OffsetCommitResponseTopic.OffsetCommitResponsePartition{
            .{ .partition_index = 0, .error_code = 0 },
            .{ .partition_index = 2, .error_code = 30 },
        };
        const topics = [_]OffsetCommitResponse.OffsetCommitResponseTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetCommitResponse{
            .throttle_time_ms = 5,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(OffsetCommitResponse, value, 8, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',  'a',  0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x1e, 0x00, 0x00, 0x00,
        });
    }

    {
        const OffsetFetchResponse = generated.offset_fetch_response.OffsetFetchResponse;
        const partitions = [_]OffsetFetchResponse.OffsetFetchResponseTopic.OffsetFetchResponsePartition{
            .{
                .partition_index = 0,
                .committed_offset = 42,
                .committed_leader_epoch = 9,
                .metadata = "meta-a",
                .error_code = 0,
            },
            .{
                .partition_index = 2,
                .committed_offset = -1,
                .committed_leader_epoch = -1,
                .metadata = null,
                .error_code = 3,
            },
        };
        const topics = [_]OffsetFetchResponse.OffsetFetchResponseTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetFetchResponse{
            .throttle_time_ms = 5,
            .topics = &topics,
            .error_code = 0,
        };
        try expectGoldenRoundTrip(OffsetFetchResponse, value, 7, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',  'a',  0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x2a, 0x00, 0x00, 0x00, 0x09, 0x07, 'm',
            'e',  't',  'a',  '-',  'a',  0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
        });
    }

    {
        const OffsetFetchResponse = generated.offset_fetch_response.OffsetFetchResponse;
        const partitions = [_]OffsetFetchResponse.OffsetFetchResponseGroup.OffsetFetchResponseTopics.OffsetFetchResponsePartitions{
            .{
                .partition_index = 0,
                .committed_offset = 42,
                .committed_leader_epoch = 9,
                .metadata = "meta-a",
                .error_code = 0,
            },
            .{
                .partition_index = 2,
                .committed_offset = -1,
                .committed_leader_epoch = -1,
                .metadata = null,
                .error_code = 3,
            },
        };
        const topics = [_]OffsetFetchResponse.OffsetFetchResponseGroup.OffsetFetchResponseTopics{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const groups = [_]OffsetFetchResponse.OffsetFetchResponseGroup{.{
            .group_id = "group-a",
            .topics = &topics,
            .error_code = 0,
        }};
        const value = OffsetFetchResponse{
            .throttle_time_ms = 5,
            .groups = &groups,
        };
        try expectGoldenRoundTrip(OffsetFetchResponse, value, 8, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x02, 0x08, 'g',  'r',
            'o',  'u',  'p',  '-',  'a',  0x02, 0x08, 't',
            'o',  'p',  'i',  'c',  '-',  'a',  0x03, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x09, 0x07,
            'm',  'e',  't',  'a',  '-',  'a',  0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        });
    }

    {
        const TxnOffsetCommitRequest = generated.txn_offset_commit_request.TxnOffsetCommitRequest;
        const partitions = [_]TxnOffsetCommitRequest.TxnOffsetCommitRequestTopic.TxnOffsetCommitRequestPartition{.{
            .partition_index = 2,
            .committed_offset = 42,
            .committed_leader_epoch = 9,
            .committed_metadata = "meta-a",
        }};
        const topics = [_]TxnOffsetCommitRequest.TxnOffsetCommitRequestTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = TxnOffsetCommitRequest{
            .transactional_id = "txn-a",
            .group_id = "group-a",
            .producer_id = 123456789,
            .producer_epoch = 2,
            .generation_id = 7,
            .member_id = "member-a",
            .group_instance_id = "instance-a",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(TxnOffsetCommitRequest, value, 3, &[_]u8{
            0x06, 't',  'x',  'n',  '-',  'a',  0x08, 'g',
            'r',  'o',  'u',  'p',  '-',  'a',  0x00, 0x00,
            0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x07, 0x09, 'm',  'e',  'm',
            'b',  'e',  'r',  '-',  'a',  0x0b, 'i',  'n',
            's',  't',  'a',  'n',  'c',  'e',  '-',  'a',
            0x02, 0x08, 't',  'o',  'p',  'i',  'c',  '-',
            'a',  0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00,
            0x00, 0x09, 0x07, 'm',  'e',  't',  'a',  '-',
            'a',  0x00, 0x00, 0x00,
        });
    }

    {
        const TxnOffsetCommitResponse = generated.txn_offset_commit_response.TxnOffsetCommitResponse;
        const partitions = [_]TxnOffsetCommitResponse.TxnOffsetCommitResponseTopic.TxnOffsetCommitResponsePartition{
            .{ .partition_index = 0, .error_code = 0 },
            .{ .partition_index = 2, .error_code = 30 },
        };
        const topics = [_]TxnOffsetCommitResponse.TxnOffsetCommitResponseTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = TxnOffsetCommitResponse{
            .throttle_time_ms = 5,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(TxnOffsetCommitResponse, value, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',  'a',  0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x1e, 0x00, 0x00, 0x00,
        });
    }

    {
        const InitProducerIdRequest = generated.init_producer_id_request.InitProducerIdRequest;
        const value_v1 = InitProducerIdRequest{
            .transactional_id = "txn-a",
            .transaction_timeout_ms = 45_000,
        };
        try expectGoldenRoundTrip(InitProducerIdRequest, value_v1, 1, &[_]u8{
            0x00, 0x05, 0x74, 0x78, 0x6e, 0x2d, 0x61,
            0x00, 0x00, 0xaf, 0xc8,
        });

        const value_v5 = InitProducerIdRequest{
            .transactional_id = "txn-a",
            .transaction_timeout_ms = 45_000,
            .producer_id = 123456789,
            .producer_epoch = 2,
        };
        try expectGoldenRoundTrip(InitProducerIdRequest, value_v5, 5, &[_]u8{
            0x06, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x00, 0x00,
            0xaf, 0xc8, 0x00, 0x00, 0x00, 0x00, 0x07, 0x5b,
            0xcd, 0x15, 0x00, 0x02, 0x00,
        });
    }

    {
        const InitProducerIdResponse = generated.init_producer_id_response.InitProducerIdResponse;
        const value = InitProducerIdResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .producer_id = 123456789,
            .producer_epoch = 2,
        };
        try expectGoldenRoundTrip(InitProducerIdResponse, value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00, 0x02,
            0x00,
        });
    }

    {
        const AddOffsetsToTxnRequest = generated.add_offsets_to_txn_request.AddOffsetsToTxnRequest;
        const value_v2 = AddOffsetsToTxnRequest{
            .transactional_id = "txn-a",
            .producer_id = 123456789,
            .producer_epoch = 2,
            .group_id = "group-a",
        };
        try expectGoldenRoundTrip(AddOffsetsToTxnRequest, value_v2, 2, &[_]u8{
            0x00, 0x05, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00,
            0x02, 0x00, 0x07, 0x67, 0x72, 0x6f, 0x75, 0x70,
            0x2d, 0x61,
        });

        const value_v4 = AddOffsetsToTxnRequest{
            .transactional_id = "txn-a",
            .producer_id = 123456789,
            .producer_epoch = 2,
            .group_id = "group-a",
        };
        try expectGoldenRoundTrip(AddOffsetsToTxnRequest, value_v4, 4, &[_]u8{
            0x06, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x00, 0x00,
            0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00, 0x02,
            0x08, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x2d, 0x61,
            0x00,
        });
    }

    {
        const AddOffsetsToTxnResponse = generated.add_offsets_to_txn_response.AddOffsetsToTxnResponse;
        const value = AddOffsetsToTxnResponse{
            .throttle_time_ms = 5,
            .error_code = 48,
        };
        try expectGoldenRoundTrip(AddOffsetsToTxnResponse, value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x00, 0x30, 0x00,
        });
    }

    {
        const EndTxnRequest = generated.end_txn_request.EndTxnRequest;
        const value_v2 = EndTxnRequest{
            .transactional_id = "txn-a",
            .producer_id = 123456789,
            .producer_epoch = 2,
            .committed = true,
        };
        try expectGoldenRoundTrip(EndTxnRequest, value_v2, 2, &[_]u8{
            0x00, 0x05, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00,
            0x02, 0x01,
        });

        const value_v4 = EndTxnRequest{
            .transactional_id = "txn-a",
            .producer_id = 123456789,
            .producer_epoch = 2,
            .committed = false,
        };
        try expectGoldenRoundTrip(EndTxnRequest, value_v4, 4, &[_]u8{
            0x06, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x00, 0x00,
            0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00, 0x02,
            0x00, 0x00,
        });
    }

    {
        const EndTxnResponse = generated.end_txn_response.EndTxnResponse;
        const value = EndTxnResponse{
            .throttle_time_ms = 5,
            .error_code = 48,
        };
        try expectGoldenRoundTrip(EndTxnResponse, value, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x00, 0x30, 0x00,
        });
    }

    {
        const WriteTxnMarkersRequest = generated.write_txn_markers_request.WriteTxnMarkersRequest;
        const Topic = WriteTxnMarkersRequest.WritableTxnMarker.WritableTxnMarkerTopic;
        const Marker = WriteTxnMarkersRequest.WritableTxnMarker;
        const topics = [_]Topic{.{
            .name = "topic-a",
            .partition_indexes = &[_]i32{ 0, 2 },
        }};
        const markers = [_]Marker{.{
            .producer_id = 123456789,
            .producer_epoch = 2,
            .transaction_result = true,
            .topics = &topics,
            .coordinator_epoch = 3,
        }};
        const value = WriteTxnMarkersRequest{
            .markers = &markers,
        };
        try expectGoldenRoundTrip(WriteTxnMarkersRequest, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x07, 0x5b, 0xcd, 0x15, 0x00, 0x02, 0x01, 0x00,
            0x00, 0x00, 0x01, 0x00, 0x07, 0x74, 0x6f, 0x70,
            0x69, 0x63, 0x2d, 0x61, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x03,
        });
        try expectGoldenRoundTrip(WriteTxnMarkersRequest, value, 1, &[_]u8{
            0x02, 0x00, 0x00, 0x00, 0x00, 0x07, 0x5b, 0xcd,
            0x15, 0x00, 0x02, 0x01, 0x02, 0x08, 0x74, 0x6f,
            0x70, 0x69, 0x63, 0x2d, 0x61, 0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x03, 0x00, 0x00,
        });
    }

    {
        const WriteTxnMarkersResponse = generated.write_txn_markers_response.WriteTxnMarkersResponse;
        const PartitionResult = WriteTxnMarkersResponse.WritableTxnMarkerResult.WritableTxnMarkerTopicResult.WritableTxnMarkerPartitionResult;
        const TopicResult = WriteTxnMarkersResponse.WritableTxnMarkerResult.WritableTxnMarkerTopicResult;
        const MarkerResult = WriteTxnMarkersResponse.WritableTxnMarkerResult;
        const partitions = [_]PartitionResult{
            .{ .partition_index = 0, .error_code = 0 },
            .{ .partition_index = 2, .error_code = 48 },
        };
        const topics = [_]TopicResult{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const markers = [_]MarkerResult{.{
            .producer_id = 123456789,
            .topics = &topics,
        }};
        const value = WriteTxnMarkersResponse{
            .markers = &markers,
        };
        try expectGoldenRoundTrip(WriteTxnMarkersResponse, value, 1, &[_]u8{
            0x02, 0x00, 0x00, 0x00, 0x00, 0x07, 0x5b, 0xcd,
            0x15, 0x02, 0x08, 0x74, 0x6f, 0x70, 0x69, 0x63,
            0x2d, 0x61, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x30,
            0x00, 0x00, 0x00, 0x00,
        });
    }

    {
        const DescribeProducersRequest = generated.describe_producers_request.DescribeProducersRequest;
        const partition_indexes = [_]i32{ 0, 2 };
        const topics = [_]DescribeProducersRequest.TopicRequest{.{
            .name = "topic-a",
            .partition_indexes = &partition_indexes,
        }};
        const value = DescribeProducersRequest{
            .topics = &topics,
        };
        try expectGoldenRoundTrip(DescribeProducersRequest, value, 0, &[_]u8{
            0x02, 0x08, 't',  'o',  'p',  'i',  'c',  '-',
            'a',  0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00,
        });
    }

    {
        const DescribeProducersResponse = generated.describe_producers_response.DescribeProducersResponse;
        const producers = [_]DescribeProducersResponse.TopicResponse.PartitionResponse.ProducerState{.{
            .producer_id = 123456789,
            .producer_epoch = 2,
            .last_sequence = 41,
            .last_timestamp = 1_700_000_000_000,
            .coordinator_epoch = 7,
            .current_txn_start_offset = 39,
        }};
        const partitions = [_]DescribeProducersResponse.TopicResponse.PartitionResponse{
            .{
                .partition_index = 0,
                .active_producers = &producers,
            },
            .{
                .partition_index = 2,
                .error_code = 3,
                .error_message = "unknown",
            },
        };
        const topics = [_]DescribeProducersResponse.TopicResponse{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = DescribeProducersResponse{
            .throttle_time_ms = 5,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(DescribeProducersResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x02, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',  'a',  0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x29, 0x00, 0x00,
            0x01, 0x8b, 0xcf, 0xe5, 0x68, 0x00, 0x00, 0x00,
            0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x03, 0x08, 'u',  'n',  'k',  'n',  'o',
            'w',  'n',  0x01, 0x00, 0x00, 0x00,
        });
    }

    {
        const CreatePartitionsResponse = generated.create_partitions_response.CreatePartitionsResponse;
        const results = [_]CreatePartitionsResponse.CreatePartitionsTopicResult{
            .{ .name = "topic-a", .error_code = 0, .error_message = null },
            .{ .name = "topic-b", .error_code = 37, .error_message = "bad-count" },
        };
        const value = CreatePartitionsResponse{
            .throttle_time_ms = 5,
            .results = &results,
        };
        try expectGoldenRoundTrip(CreatePartitionsResponse, value, 2, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x03, 0x08, 't',  'o',
            'p',  'i',  'c',  '-',  'a',  0x00, 0x00, 0x00,
            0x00, 0x08, 't',  'o',  'p',  'i',  'c',  '-',
            'b',  0x00, 0x25, 0x0a, 'b',  'a',  'd',  '-',
            'c',  'o',  'u',  'n',  't',  0x00, 0x00,
        });
    }

    {
        const OffsetDeleteRequest = generated.offset_delete_request.OffsetDeleteRequest;
        const partitions = [_]OffsetDeleteRequest.OffsetDeleteRequestTopic.OffsetDeleteRequestPartition{
            .{ .partition_index = 0 },
            .{ .partition_index = 2 },
        };
        const topics = [_]OffsetDeleteRequest.OffsetDeleteRequestTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetDeleteRequest{
            .group_id = "group-a",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(OffsetDeleteRequest, value, 0, &[_]u8{
            0x00, 0x07, 'g',  'r',  'o',  'u',  'p',  '-',
            'a',  0x00, 0x00, 0x00, 0x01, 0x00, 0x07, 't',
            'o',  'p',  'i',  'c',  '-',  'a',  0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x02,
        });
    }

    {
        const OffsetDeleteResponse = generated.offset_delete_response.OffsetDeleteResponse;
        const partitions = [_]OffsetDeleteResponse.OffsetDeleteResponseTopic.OffsetDeleteResponsePartition{
            .{ .partition_index = 0, .error_code = 0 },
            .{ .partition_index = 2, .error_code = 30 },
        };
        const topics = [_]OffsetDeleteResponse.OffsetDeleteResponseTopic{.{
            .name = "topic-a",
            .partitions = &partitions,
        }};
        const value = OffsetDeleteResponse{
            .error_code = 0,
            .throttle_time_ms = 5,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(OffsetDeleteResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x07, 't',  'o',  'p',  'i',
            'c',  '-',  'a',  0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x1e,
        });
    }

    {
        const ListTransactionsRequest = generated.list_transactions_request.ListTransactionsRequest;
        const state_filters = [_]?[]const u8{ "Ongoing", "PrepareCommit" };
        const producer_id_filters = [_]i64{ 123456789, 987654321 };
        const value = ListTransactionsRequest{
            .state_filters = &state_filters,
            .producer_id_filters = &producer_id_filters,
            .duration_filter = 60_000,
        };
        try expectGoldenRoundTrip(ListTransactionsRequest, value, 1, &[_]u8{
            0x03, 0x08, 'O',  'n',
            'g',  'o',  'i',  'n',
            'g',  0x0e, 'P',  'r',
            'e',  'p',  'a',  'r',
            'e',  'C',  'o',  'm',
            'm',  'i',  't',  0x03,
            0x00, 0x00, 0x00, 0x00,
            0x07, 0x5b, 0xcd, 0x15,
            0x00, 0x00, 0x00, 0x00,
            0x3a, 0xde, 0x68, 0xb1,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xea, 0x60,
            0x00,
        });
    }

    {
        const ListTransactionsResponse = generated.list_transactions_response.ListTransactionsResponse;
        const unknown_state_filters = [_]?[]const u8{"bad-state"};
        const transaction_states = [_]ListTransactionsResponse.TransactionState{.{
            .transactional_id = "txn-a",
            .producer_id = 123456789,
            .transaction_state = "Ongoing",
        }};
        const value = ListTransactionsResponse{
            .throttle_time_ms = 5,
            .error_code = 0,
            .unknown_state_filters = &unknown_state_filters,
            .transaction_states = &transaction_states,
        };
        try expectGoldenRoundTrip(ListTransactionsResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x05,
            0x00, 0x00, 0x02, 0x0a,
            'b',  'a',  'd',  '-',
            's',  't',  'a',  't',
            'e',  0x02, 0x06, 't',
            'x',  'n',  '-',  'a',
            0x00, 0x00, 0x00, 0x00,
            0x07, 0x5b, 0xcd, 0x15,
            0x08, 'O',  'n',  'g',
            'o',  'i',  'n',  'g',
            0x00, 0x00,
        });
    }

    {
        const GetTelemetrySubscriptionsRequest = generated.get_telemetry_subscriptions_request.GetTelemetrySubscriptionsRequest;
        const value = GetTelemetrySubscriptionsRequest{
            .client_instance_id = .{
                0x00, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x77,
                0x88, 0x99, 0xaa, 0xbb,
                0xcc, 0xdd, 0xee, 0xff,
            },
        };
        try expectGoldenRoundTrip(GetTelemetrySubscriptionsRequest, value, 0, &[_]u8{
            0x00, 0x11, 0x22, 0x33,
            0x44, 0x55, 0x66, 0x77,
            0x88, 0x99, 0xaa, 0xbb,
            0xcc, 0xdd, 0xee, 0xff,
            0x00,
        });
    }

    {
        const GetTelemetrySubscriptionsResponse = generated.get_telemetry_subscriptions_response.GetTelemetrySubscriptionsResponse;
        const requested_metrics = [_]?[]const u8{ "", "kafka.server" };
        const value = GetTelemetrySubscriptionsResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .client_instance_id = .{
                0x00, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x77,
                0x88, 0x99, 0xaa, 0xbb,
                0xcc, 0xdd, 0xee, 0xff,
            },
            .subscription_id = 42,
            .accepted_compression_types = &[_]i8{ 0, 1 },
            .push_interval_ms = 5000,
            .telemetry_max_bytes = 1_048_576,
            .delta_temporality = true,
            .requested_metrics = &requested_metrics,
        };
        try expectGoldenRoundTrip(GetTelemetrySubscriptionsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x11,
            0x22, 0x33, 0x44, 0x55,
            0x66, 0x77, 0x88, 0x99,
            0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x00, 0x00,
            0x00, 0x2a, 0x03, 0x00,
            0x01, 0x00, 0x00, 0x13,
            0x88, 0x00, 0x10, 0x00,
            0x00, 0x01, 0x03, 0x01,
            0x0d, 'k',  'a',  'f',
            'k',  'a',  '.',  's',
            'e',  'r',  'v',  'e',
            'r',  0x00,
        });
    }

    {
        const PushTelemetryRequest = generated.push_telemetry_request.PushTelemetryRequest;
        const value = PushTelemetryRequest{
            .client_instance_id = .{
                0x00, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x77,
                0x88, 0x99, 0xaa, 0xbb,
                0xcc, 0xdd, 0xee, 0xff,
            },
            .subscription_id = 42,
            .terminating = true,
            .compression_type = 1,
            .metrics = &[_]u8{ 0xde, 0xad, 0xbe, 0xef },
        };
        try expectGoldenRoundTrip(PushTelemetryRequest, value, 0, &[_]u8{
            0x00, 0x11, 0x22, 0x33,
            0x44, 0x55, 0x66, 0x77,
            0x88, 0x99, 0xaa, 0xbb,
            0xcc, 0xdd, 0xee, 0xff,
            0x00, 0x00, 0x00, 0x2a,
            0x01, 0x01, 0x05, 0xde,
            0xad, 0xbe, 0xef, 0x00,
        });
    }

    {
        const PushTelemetryResponse = generated.push_telemetry_response.PushTelemetryResponse;
        const value = PushTelemetryResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
        };
        try expectGoldenRoundTrip(PushTelemetryResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00,
        });
    }

    {
        const ListClientMetricsResourcesRequest = generated.list_client_metrics_resources_request.ListClientMetricsResourcesRequest;
        const value = ListClientMetricsResourcesRequest{};
        try expectGoldenRoundTrip(ListClientMetricsResourcesRequest, value, 0, &[_]u8{
            0x00,
        });
    }

    {
        const ListClientMetricsResourcesResponse = generated.list_client_metrics_resources_response.ListClientMetricsResourcesResponse;
        const resources = [_]ListClientMetricsResourcesResponse.ClientMetricsResource{
            .{ .name = "__default__" },
            .{ .name = "client-A" },
        };
        const value = ListClientMetricsResourcesResponse{
            .throttle_time_ms = 7,
            .error_code = 0,
            .client_metrics_resources = &resources,
        };
        try expectGoldenRoundTrip(ListClientMetricsResourcesResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x03, 0x0c,
            '_',  '_',  'd',  'e',
            'f',  'a',  'u',  'l',
            't',  '_',  '_',  0x00,
            0x09, 'c',  'l',  'i',
            'e',  'n',  't',  '-',
            'A',  0x00, 0x00,
        });
    }

    {
        const AddPartitionsToTxnRequest = generated.add_partitions_to_txn_request.AddPartitionsToTxnRequest;
        const AddPartitionsToTxnTopic = generated.add_partitions_to_txn_request.AddPartitionsToTxnTopic;
        const topics = [_]AddPartitionsToTxnTopic{.{
            .name = "topic-a",
            .partitions = &[_]i32{ 0, 2 },
        }};
        const value_v3 = AddPartitionsToTxnRequest{
            .v3_and_below_transactional_id = "txn-a",
            .v3_and_below_producer_id = 123456789,
            .v3_and_below_producer_epoch = 2,
            .v3_and_below_topics = &topics,
        };
        try expectGoldenRoundTrip(AddPartitionsToTxnRequest, value_v3, 3, &[_]u8{
            0x06, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x00, 0x00,
            0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00, 0x02,
            0x02, 0x08, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x2d,
            0x61, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00,
        });

        const transactions = [_]AddPartitionsToTxnRequest.AddPartitionsToTxnTransaction{.{
            .transactional_id = "txn-a",
            .producer_id = 123456789,
            .producer_epoch = 2,
            .verify_only = true,
            .topics = &topics,
        }};
        const value_v4 = AddPartitionsToTxnRequest{
            .transactions = &transactions,
        };
        try expectGoldenRoundTrip(AddPartitionsToTxnRequest, value_v4, 4, &[_]u8{
            0x02, 0x06, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x00,
            0x02, 0x01, 0x02, 0x08, 0x74, 0x6f, 0x70, 0x69,
            0x63, 0x2d, 0x61, 0x03, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        });
    }

    {
        const AddPartitionsToTxnResponse = generated.add_partitions_to_txn_response.AddPartitionsToTxnResponse;
        const AddPartitionsToTxnTopicResult = generated.add_partitions_to_txn_response.AddPartitionsToTxnTopicResult;
        const AddPartitionsToTxnPartitionResult = generated.add_partitions_to_txn_response.AddPartitionsToTxnPartitionResult;
        const partition_results = [_]AddPartitionsToTxnPartitionResult{
            .{ .partition_index = 0, .partition_error_code = 0 },
            .{ .partition_index = 2, .partition_error_code = 48 },
        };
        const topic_results = [_]AddPartitionsToTxnTopicResult{.{
            .name = "topic-a",
            .results_by_partition = &partition_results,
        }};
        const value_v3 = AddPartitionsToTxnResponse{
            .throttle_time_ms = 5,
            .results_by_topic_v3_and_below = &topic_results,
        };
        try expectGoldenRoundTrip(AddPartitionsToTxnResponse, value_v3, 3, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x02, 0x08, 0x74, 0x6f,
            0x70, 0x69, 0x63, 0x2d, 0x61, 0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x30, 0x00, 0x00, 0x00,
        });

        const transaction_results = [_]AddPartitionsToTxnResponse.AddPartitionsToTxnResult{.{
            .transactional_id = "txn-a",
            .topic_results = &topic_results,
        }};
        const value_v4 = AddPartitionsToTxnResponse{
            .throttle_time_ms = 5,
            .error_code = 0,
            .results_by_transaction = &transaction_results,
        };
        try expectGoldenRoundTrip(AddPartitionsToTxnResponse, value_v4, 4, &[_]u8{
            0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x02, 0x06,
            0x74, 0x78, 0x6e, 0x2d, 0x61, 0x02, 0x08, 0x74,
            0x6f, 0x70, 0x69, 0x63, 0x2d, 0x61, 0x03, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00,
        });
    }

    {
        const AssignReplicasToDirsRequest = generated.assign_replicas_to_dirs_request.AssignReplicasToDirsRequest;
        const partition_data = [_]AssignReplicasToDirsRequest.DirectoryData.TopicData.PartitionData{
            .{ .partition_index = 3 },
            .{ .partition_index = 7 },
        };
        const topics = [_]AssignReplicasToDirsRequest.DirectoryData.TopicData{.{
            .topic_id = .{
                0x10, 0x11, 0x12, 0x13,
                0x14, 0x15, 0x16, 0x17,
                0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            },
            .partitions = &partition_data,
        }};
        const directories = [_]AssignReplicasToDirsRequest.DirectoryData{.{
            .id = .{
                0x00, 0x01, 0x02, 0x03,
                0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b,
                0x0c, 0x0d, 0x0e, 0x0f,
            },
            .topics = &topics,
        }};
        const value = AssignReplicasToDirsRequest{
            .broker_id = 2,
            .broker_epoch = 9,
            .directories = &directories,
        };
        try expectGoldenRoundTrip(AssignReplicasToDirsRequest, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x09, 0x02, 0x00, 0x01, 0x02,
            0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
            0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x02, 0x10, 0x11,
            0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
            0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x03, 0x00,
            0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00,
        });
    }

    {
        const AssignReplicasToDirsResponse = generated.assign_replicas_to_dirs_response.AssignReplicasToDirsResponse;
        const partition_data = [_]AssignReplicasToDirsResponse.DirectoryData.TopicData.PartitionData{
            .{ .partition_index = 3, .error_code = 0 },
            .{ .partition_index = 7, .error_code = 72 },
        };
        const topics = [_]AssignReplicasToDirsResponse.DirectoryData.TopicData{.{
            .topic_id = .{
                0x10, 0x11, 0x12, 0x13,
                0x14, 0x15, 0x16, 0x17,
                0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            },
            .partitions = &partition_data,
        }};
        const directories = [_]AssignReplicasToDirsResponse.DirectoryData{.{
            .id = .{
                0x00, 0x01, 0x02, 0x03,
                0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b,
                0x0c, 0x0d, 0x0e, 0x0f,
            },
            .topics = &topics,
        }};
        const value = AssignReplicasToDirsResponse{
            .throttle_time_ms = 11,
            .error_code = 0,
            .directories = &directories,
        };
        try expectGoldenRoundTrip(AssignReplicasToDirsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x02, 0x00,
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x02,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
            0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x00, 0x48, 0x00, 0x00,
            0x00, 0x00,
        });
    }

    {
        const DescribeTransactionsRequest = generated.describe_transactions_request.DescribeTransactionsRequest;
        const transactional_ids = [_]?[]const u8{ "txn-a", "txn-b" };
        const value = DescribeTransactionsRequest{
            .transactional_ids = &transactional_ids,
        };
        try expectGoldenRoundTrip(DescribeTransactionsRequest, value, 0, &[_]u8{
            0x03, 0x06, 0x74, 0x78, 0x6e, 0x2d, 0x61, 0x06,
            0x74, 0x78, 0x6e, 0x2d, 0x62, 0x00,
        });
    }

    {
        const DescribeTransactionsResponse = generated.describe_transactions_response.DescribeTransactionsResponse;
        const topics = [_]DescribeTransactionsResponse.TransactionState.TopicData{.{
            .topic = "topic-a",
            .partitions = &[_]i32{ 0, 2 },
        }};
        const transaction_states = [_]DescribeTransactionsResponse.TransactionState{
            .{
                .error_code = 0,
                .transactional_id = "txn-a",
                .transaction_state = "Ongoing",
                .transaction_timeout_ms = 45_000,
                .transaction_start_time_ms = 1_700_000_000_000,
                .producer_id = 123456789,
                .producer_epoch = 2,
                .topics = &topics,
            },
            .{
                .error_code = 48,
                .transactional_id = "missing",
                .transaction_state = "",
                .transaction_timeout_ms = 0,
                .transaction_start_time_ms = -1,
                .producer_id = -1,
                .producer_epoch = -1,
            },
        };
        const value = DescribeTransactionsResponse{
            .throttle_time_ms = 7,
            .transaction_states = &transaction_states,
        };
        try expectGoldenRoundTrip(DescribeTransactionsResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x07, 0x03, 0x00, 0x00, 0x06,
            0x74, 0x78, 0x6e, 0x2d, 0x61, 0x08, 0x4f, 0x6e,
            0x67, 0x6f, 0x69, 0x6e, 0x67, 0x00, 0x00, 0xaf,
            0xc8, 0x00, 0x00, 0x01, 0x8b, 0xcf, 0xe5, 0x68,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x5b, 0xcd,
            0x15, 0x00, 0x02, 0x02, 0x08, 0x74, 0x6f, 0x70,
            0x69, 0x63, 0x2d, 0x61, 0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
            0x30, 0x08, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6e,
            0x67, 0x01, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0x01, 0x00, 0x00,
        });
    }

    {
        const ConsumerGroupHeartbeatRequest = generated.consumer_group_heartbeat_request.ConsumerGroupHeartbeatRequest;
        const null_value = ConsumerGroupHeartbeatRequest{
            .group_id = "g",
            .member_id = "m",
            .member_epoch = 0,
            .instance_id = null,
            .rack_id = null,
            .rebalance_timeout_ms = 30_000,
            .subscribed_topic_names = null,
            .server_assignor = null,
            .topic_partitions = null,
        };
        try expectGoldenRoundTrip(ConsumerGroupHeartbeatRequest, null_value, 0, &[_]u8{
            0x02, 'g',  0x02, 'm',
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x75, 0x30, 0x00, 0x00,
            0x00, 0x00,
        });

        const empty_topic_names = [_]?[]const u8{};
        const empty_topic_partitions = [_]ConsumerGroupHeartbeatRequest.TopicPartitions{};
        const empty_value = ConsumerGroupHeartbeatRequest{
            .group_id = "g",
            .member_id = "m",
            .member_epoch = 0,
            .instance_id = null,
            .rack_id = null,
            .rebalance_timeout_ms = 30_000,
            .subscribed_topic_names = &empty_topic_names,
            .server_assignor = null,
            .topic_partitions = &empty_topic_partitions,
        };
        try expectGoldenRoundTrip(ConsumerGroupHeartbeatRequest, empty_value, 0, &[_]u8{
            0x02, 'g',  0x02, 'm',
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x75, 0x30, 0x01, 0x00,
            0x01, 0x00,
        });
    }

    {
        const ShareGroupHeartbeatRequest = generated.share_group_heartbeat_request.ShareGroupHeartbeatRequest;
        const null_value = ShareGroupHeartbeatRequest{
            .group_id = "g",
            .member_id = "m",
            .member_epoch = 0,
            .rack_id = null,
            .subscribed_topic_names = null,
        };
        try expectGoldenRoundTrip(ShareGroupHeartbeatRequest, null_value, 0, &[_]u8{
            0x02, 'g',  0x02, 'm',
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00,
        });

        const empty_topic_names = [_]?[]const u8{};
        const empty_value = ShareGroupHeartbeatRequest{
            .group_id = "g",
            .member_id = "m",
            .member_epoch = 0,
            .rack_id = null,
            .subscribed_topic_names = &empty_topic_names,
        };
        try expectGoldenRoundTrip(ShareGroupHeartbeatRequest, empty_value, 0, &[_]u8{
            0x02, 'g',  0x02, 'm',
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00,
        });
    }

    {
        const ShareFetchRequest = generated.share_fetch_request.ShareFetchRequest;
        const acknowledgement_batches = [_]ShareFetchRequest.FetchTopic.FetchPartition.AcknowledgementBatch{.{
            .first_offset = 5,
            .last_offset = 7,
            .acknowledge_types = &[_]i8{ 1, 2 },
        }};
        const partitions = [_]ShareFetchRequest.FetchTopic.FetchPartition{.{
            .partition_index = 0,
            .partition_max_bytes = 1024,
            .acknowledgement_batches = &acknowledgement_batches,
        }};
        const topics = [_]ShareFetchRequest.FetchTopic{.{
            .topic_id = .{
                0x20, 0x21, 0x22, 0x23,
                0x24, 0x25, 0x26, 0x27,
                0x28, 0x29, 0x2a, 0x2b,
                0x2c, 0x2d, 0x2e, 0x2f,
            },
            .partitions = &partitions,
        }};
        const forgotten_topics = [_]ShareFetchRequest.ForgottenTopic{.{
            .topic_id = .{
                0x30, 0x31, 0x32, 0x33,
                0x34, 0x35, 0x36, 0x37,
                0x38, 0x39, 0x3a, 0x3b,
                0x3c, 0x3d, 0x3e, 0x3f,
            },
            .partitions = &[_]i32{3},
        }};
        const value = ShareFetchRequest{
            .group_id = "share-g",
            .member_id = "member-1",
            .share_session_epoch = 2,
            .max_wait_ms = 500,
            .min_bytes = 1,
            .max_bytes = 4096,
            .topics = &topics,
            .forgotten_topics_data = &forgotten_topics,
        };
        try expectGoldenRoundTrip(ShareFetchRequest, value, 0, &[_]u8{
            0x08, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2d, 0x67,
            0x09, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x2d,
            0x31, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01,
            0xf4, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x10,
            0x00, 0x02, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25,
            0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
            0x2e, 0x2f, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x04, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x03, 0x01, 0x02, 0x00,
            0x00, 0x00, 0x02, 0x30, 0x31, 0x32, 0x33, 0x34,
            0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c,
            0x3d, 0x3e, 0x3f, 0x02, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00,
        });
    }

    {
        const ShareFetchResponse = generated.share_fetch_response.ShareFetchResponse;
        const acquired_records = [_]ShareFetchResponse.ShareFetchableTopicResponse.PartitionData.AcquiredRecords{.{
            .first_offset = 5,
            .last_offset = 7,
            .delivery_count = 2,
        }};
        const partition_data = [_]ShareFetchResponse.ShareFetchableTopicResponse.PartitionData{.{
            .partition_index = 0,
            .error_code = 0,
            .error_message = null,
            .acknowledge_error_code = 0,
            .acknowledge_error_message = null,
            .current_leader = .{ .leader_id = 2, .leader_epoch = 9 },
            .records = "abc",
            .acquired_records = &acquired_records,
        }};
        const responses = [_]ShareFetchResponse.ShareFetchableTopicResponse{.{
            .topic_id = .{
                0x20, 0x21, 0x22, 0x23,
                0x24, 0x25, 0x26, 0x27,
                0x28, 0x29, 0x2a, 0x2b,
                0x2c, 0x2d, 0x2e, 0x2f,
            },
            .partitions = &partition_data,
        }};
        const node_endpoints = [_]ShareFetchResponse.NodeEndpoint{.{
            .node_id = 2,
            .host = "broker-2",
            .port = 19092,
            .rack = "az-a",
        }};
        const value = ShareFetchResponse{
            .throttle_time_ms = 3,
            .error_code = 0,
            .error_message = null,
            .responses = &responses,
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(ShareFetchResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
            0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x09, 0x00, 0x04, 0x61, 0x62, 0x63,
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x02, 0x09, 0x62, 0x72, 0x6f, 0x6b,
            0x65, 0x72, 0x2d, 0x32, 0x00, 0x00, 0x4a, 0x94,
            0x05, 0x61, 0x7a, 0x2d, 0x61, 0x00, 0x00,
        });
    }

    {
        const ShareAcknowledgeRequest = generated.share_acknowledge_request.ShareAcknowledgeRequest;
        const acknowledgement_batches = [_]ShareAcknowledgeRequest.AcknowledgeTopic.AcknowledgePartition.AcknowledgementBatch{.{
            .first_offset = 5,
            .last_offset = 7,
            .acknowledge_types = &[_]i8{ 1, 3 },
        }};
        const partitions = [_]ShareAcknowledgeRequest.AcknowledgeTopic.AcknowledgePartition{.{
            .partition_index = 0,
            .acknowledgement_batches = &acknowledgement_batches,
        }};
        const topics = [_]ShareAcknowledgeRequest.AcknowledgeTopic{.{
            .topic_id = .{
                0x20, 0x21, 0x22, 0x23,
                0x24, 0x25, 0x26, 0x27,
                0x28, 0x29, 0x2a, 0x2b,
                0x2c, 0x2d, 0x2e, 0x2f,
            },
            .partitions = &partitions,
        }};
        const value = ShareAcknowledgeRequest{
            .group_id = "share-g",
            .member_id = "member-1",
            .share_session_epoch = 2,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(ShareAcknowledgeRequest, value, 0, &[_]u8{
            0x08, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2d, 0x67,
            0x09, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x2d,
            0x31, 0x00, 0x00, 0x00, 0x02, 0x02, 0x20, 0x21,
            0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
            0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x03, 0x01, 0x03, 0x00,
            0x00, 0x00, 0x00,
        });
    }

    {
        const ShareAcknowledgeResponse = generated.share_acknowledge_response.ShareAcknowledgeResponse;
        const partition_data = [_]ShareAcknowledgeResponse.ShareAcknowledgeTopicResponse.PartitionData{.{
            .partition_index = 0,
            .error_code = 6,
            .error_message = "fenced",
            .current_leader = .{ .leader_id = 2, .leader_epoch = 9 },
        }};
        const responses = [_]ShareAcknowledgeResponse.ShareAcknowledgeTopicResponse{.{
            .topic_id = .{
                0x20, 0x21, 0x22, 0x23,
                0x24, 0x25, 0x26, 0x27,
                0x28, 0x29, 0x2a, 0x2b,
                0x2c, 0x2d, 0x2e, 0x2f,
            },
            .partitions = &partition_data,
        }};
        const node_endpoints = [_]ShareAcknowledgeResponse.NodeEndpoint{.{
            .node_id = 2,
            .host = "broker-2",
            .port = 19092,
            .rack = "az-a",
        }};
        const value = ShareAcknowledgeResponse{
            .throttle_time_ms = 4,
            .error_code = 0,
            .error_message = "ack-ok",
            .responses = &responses,
            .node_endpoints = &node_endpoints,
        };
        try expectGoldenRoundTrip(ShareAcknowledgeResponse, value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x07, 0x61,
            0x63, 0x6b, 0x2d, 0x6f, 0x6b, 0x02, 0x20, 0x21,
            0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
            0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x06, 0x07, 0x66, 0x65,
            0x6e, 0x63, 0x65, 0x64, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x02, 0x09, 0x62, 0x72, 0x6f,
            0x6b, 0x65, 0x72, 0x2d, 0x32, 0x00, 0x00, 0x4a,
            0x94, 0x05, 0x61, 0x7a, 0x2d, 0x61, 0x00, 0x00,
        });
    }

    {
        const InitializeShareGroupStateRequest = generated.initialize_share_group_state_request.InitializeShareGroupStateRequest;
        const partitions = [_]InitializeShareGroupStateRequest.InitializeStateData.PartitionData{
            .{ .partition = 0, .state_epoch = 5, .start_offset = 10 },
            .{ .partition = 2, .state_epoch = 6, .start_offset = -1 },
        };
        const topics = [_]InitializeShareGroupStateRequest.InitializeStateData{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partitions,
        }};
        const value = InitializeShareGroupStateRequest{
            .group_id = "share-g",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(InitializeShareGroupStateRequest, value, 0, &[_]u8{
            0x08, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2d, 0x67,
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x06, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
        });
    }

    {
        const InitializeShareGroupStateResponse = generated.initialize_share_group_state_response.InitializeShareGroupStateResponse;
        const partition_results = [_]InitializeShareGroupStateResponse.InitializeStateResult.PartitionResult{
            .{ .partition = 0, .error_code = 0, .error_message = null },
            .{ .partition = 2, .error_code = 42, .error_message = "bad-epoch" },
        };
        const results = [_]InitializeShareGroupStateResponse.InitializeStateResult{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partition_results,
        }};
        const value = InitializeShareGroupStateResponse{
            .results = &results,
        };
        try expectGoldenRoundTrip(InitializeShareGroupStateResponse, value, 0, &[_]u8{
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x2a,
            0x0a, 0x62, 0x61, 0x64, 0x2d, 0x65, 0x70, 0x6f,
            0x63, 0x68, 0x00, 0x00, 0x00,
        });
    }

    {
        const ReadShareGroupStateRequest = generated.read_share_group_state_request.ReadShareGroupStateRequest;
        const partitions = [_]ReadShareGroupStateRequest.ReadStateData.PartitionData{
            .{ .partition = 0, .leader_epoch = 9 },
            .{ .partition = 2, .leader_epoch = 9 },
        };
        const topics = [_]ReadShareGroupStateRequest.ReadStateData{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partitions,
        }};
        const value = ReadShareGroupStateRequest{
            .group_id = "share-g",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(ReadShareGroupStateRequest, value, 0, &[_]u8{
            0x08, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2d, 0x67,
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x09, 0x00, 0x00, 0x00,
        });
    }

    {
        const ReadShareGroupStateResponse = generated.read_share_group_state_response.ReadShareGroupStateResponse;
        const state_batches = [_]ReadShareGroupStateResponse.ReadStateResult.PartitionResult.StateBatch{
            .{ .first_offset = 10, .last_offset = 11, .delivery_state = 0, .delivery_count = 1 },
            .{ .first_offset = 12, .last_offset = 12, .delivery_state = 2, .delivery_count = 2 },
        };
        const partition_results = [_]ReadShareGroupStateResponse.ReadStateResult.PartitionResult{
            .{
                .partition = 0,
                .error_code = 0,
                .error_message = null,
                .state_epoch = 5,
                .start_offset = 10,
                .state_batches = &state_batches,
            },
            .{
                .partition = 2,
                .error_code = 42,
                .error_message = "bad-epoch",
                .state_epoch = 0,
                .start_offset = -1,
            },
        };
        const results = [_]ReadShareGroupStateResponse.ReadStateResult{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partition_results,
        }};
        const value = ReadShareGroupStateResponse{
            .results = &results,
        };
        try expectGoldenRoundTrip(ReadShareGroupStateResponse, value, 0, &[_]u8{
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x0a, 0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x0c, 0x02, 0x00, 0x02, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x2a, 0x0a, 0x62, 0x61,
            0x64, 0x2d, 0x65, 0x70, 0x6f, 0x63, 0x68, 0x00,
            0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00,
        });
    }

    {
        const WriteShareGroupStateRequest = generated.write_share_group_state_request.WriteShareGroupStateRequest;
        const state_batches = [_]WriteShareGroupStateRequest.WriteStateData.PartitionData.StateBatch{
            .{ .first_offset = 10, .last_offset = 11, .delivery_state = 0, .delivery_count = 1 },
            .{ .first_offset = 12, .last_offset = 12, .delivery_state = 2, .delivery_count = 2 },
        };
        const partitions = [_]WriteShareGroupStateRequest.WriteStateData.PartitionData{
            .{
                .partition = 0,
                .state_epoch = 5,
                .leader_epoch = 9,
                .start_offset = 10,
                .state_batches = &state_batches,
            },
            .{
                .partition = 2,
                .state_epoch = 6,
                .leader_epoch = 9,
                .start_offset = -1,
            },
        };
        const topics = [_]WriteShareGroupStateRequest.WriteStateData{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partitions,
        }};
        const value = WriteShareGroupStateRequest{
            .group_id = "share-g",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(WriteShareGroupStateRequest, value, 0, &[_]u8{
            0x08, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2d, 0x67,
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x05, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x03, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x0c, 0x02, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x06,
            0x00, 0x00, 0x00, 0x09, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00,
        });
    }

    {
        const WriteShareGroupStateResponse = generated.write_share_group_state_response.WriteShareGroupStateResponse;
        const partition_results = [_]WriteShareGroupStateResponse.WriteStateResult.PartitionResult{
            .{ .partition = 0, .error_code = 0, .error_message = null },
            .{ .partition = 2, .error_code = 42, .error_message = "bad-epoch" },
        };
        const results = [_]WriteShareGroupStateResponse.WriteStateResult{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partition_results,
        }};
        const value = WriteShareGroupStateResponse{
            .results = &results,
        };
        try expectGoldenRoundTrip(WriteShareGroupStateResponse, value, 0, &[_]u8{
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x2a,
            0x0a, 0x62, 0x61, 0x64, 0x2d, 0x65, 0x70, 0x6f,
            0x63, 0x68, 0x00, 0x00, 0x00,
        });
    }

    {
        const DeleteShareGroupStateRequest = generated.delete_share_group_state_request.DeleteShareGroupStateRequest;
        const partitions = [_]DeleteShareGroupStateRequest.DeleteStateData.PartitionData{
            .{ .partition = 0 },
            .{ .partition = 2 },
        };
        const topics = [_]DeleteShareGroupStateRequest.DeleteStateData{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partitions,
        }};
        const value = DeleteShareGroupStateRequest{
            .group_id = "share-g",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(DeleteShareGroupStateRequest, value, 0, &[_]u8{
            0x08, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2d, 0x67,
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        });
    }

    {
        const DeleteShareGroupStateResponse = generated.delete_share_group_state_response.DeleteShareGroupStateResponse;
        const partition_results = [_]DeleteShareGroupStateResponse.DeleteStateResult.PartitionResult{
            .{ .partition = 0, .error_code = 0, .error_message = null },
            .{ .partition = 2, .error_code = 42, .error_message = "bad-epoch" },
        };
        const results = [_]DeleteShareGroupStateResponse.DeleteStateResult{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partition_results,
        }};
        const value = DeleteShareGroupStateResponse{
            .results = &results,
        };
        try expectGoldenRoundTrip(DeleteShareGroupStateResponse, value, 0, &[_]u8{
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x2a,
            0x0a, 0x62, 0x61, 0x64, 0x2d, 0x65, 0x70, 0x6f,
            0x63, 0x68, 0x00, 0x00, 0x00,
        });
    }

    {
        const ReadShareGroupStateSummaryRequest = generated.read_share_group_state_summary_request.ReadShareGroupStateSummaryRequest;
        const partitions = [_]ReadShareGroupStateSummaryRequest.ReadStateSummaryData.PartitionData{
            .{ .partition = 0, .leader_epoch = 9 },
            .{ .partition = 2, .leader_epoch = 9 },
        };
        const topics = [_]ReadShareGroupStateSummaryRequest.ReadStateSummaryData{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partitions,
        }};
        const value = ReadShareGroupStateSummaryRequest{
            .group_id = "share-g",
            .topics = &topics,
        };
        try expectGoldenRoundTrip(ReadShareGroupStateSummaryRequest, value, 0, &[_]u8{
            0x08, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2d, 0x67,
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x09, 0x00, 0x00, 0x00,
        });
    }

    {
        const ReadShareGroupStateSummaryResponse = generated.read_share_group_state_summary_response.ReadShareGroupStateSummaryResponse;
        const partition_results = [_]ReadShareGroupStateSummaryResponse.ReadStateSummaryResult.PartitionResult{
            .{
                .partition = 0,
                .error_code = 0,
                .error_message = null,
                .state_epoch = 5,
                .start_offset = 10,
            },
            .{
                .partition = 2,
                .error_code = 42,
                .error_message = "bad-epoch",
                .state_epoch = 0,
                .start_offset = -1,
            },
        };
        const results = [_]ReadShareGroupStateSummaryResponse.ReadStateSummaryResult{.{
            .topic_id = .{
                0x40, 0x41, 0x42, 0x43,
                0x44, 0x45, 0x46, 0x47,
                0x48, 0x49, 0x4a, 0x4b,
                0x4c, 0x4d, 0x4e, 0x4f,
            },
            .partitions = &partition_results,
        }};
        const value = ReadShareGroupStateSummaryResponse{
            .results = &results,
        };
        try expectGoldenRoundTrip(ReadShareGroupStateSummaryResponse, value, 0, &[_]u8{
            0x02, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
            0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
            0x4f, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x2a, 0x0a, 0x62, 0x61, 0x64,
            0x2d, 0x65, 0x70, 0x6f, 0x63, 0x68, 0x00, 0x00,
            0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0x00, 0x00, 0x00,
        });
    }

    {
        const AlterPartitionReassignmentsRequest = generated.alter_partition_reassignments_request.AlterPartitionReassignmentsRequest;
        const null_partitions = [_]AlterPartitionReassignmentsRequest.ReassignableTopic.ReassignablePartition{.{
            .partition_index = 0,
            .replicas = null,
        }};
        const null_topics = [_]AlterPartitionReassignmentsRequest.ReassignableTopic{.{
            .name = "r",
            .partitions = &null_partitions,
        }};
        const null_value = AlterPartitionReassignmentsRequest{
            .timeout_ms = 1000,
            .topics = &null_topics,
        };
        try expectGoldenRoundTrip(AlterPartitionReassignmentsRequest, null_value, 0, &[_]u8{
            0x00, 0x00, 0x03, 0xe8, 0x02, 0x02, 'r',  0x02,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        });

        const empty_partitions = [_]AlterPartitionReassignmentsRequest.ReassignableTopic.ReassignablePartition{.{
            .partition_index = 0,
            .replicas = &.{},
        }};
        const empty_topics = [_]AlterPartitionReassignmentsRequest.ReassignableTopic{.{
            .name = "r",
            .partitions = &empty_partitions,
        }};
        const empty_value = AlterPartitionReassignmentsRequest{
            .timeout_ms = 1000,
            .topics = &empty_topics,
        };
        try expectGoldenRoundTrip(AlterPartitionReassignmentsRequest, empty_value, 0, &[_]u8{
            0x00, 0x00, 0x03, 0xe8, 0x02, 0x02, 'r',  0x02,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        });
    }

    {
        const ListPartitionReassignmentsRequest = generated.list_partition_reassignments_request.ListPartitionReassignmentsRequest;
        const null_value = ListPartitionReassignmentsRequest{
            .timeout_ms = 1000,
            .topics = null,
        };
        try expectGoldenRoundTrip(ListPartitionReassignmentsRequest, null_value, 0, &[_]u8{
            0x00, 0x00, 0x03, 0xe8, 0x00, 0x00,
        });

        const empty_value = ListPartitionReassignmentsRequest{
            .timeout_ms = 1000,
            .topics = &.{},
        };
        try expectGoldenRoundTrip(ListPartitionReassignmentsRequest, empty_value, 0, &[_]u8{
            0x00, 0x00, 0x03, 0xe8, 0x01, 0x00,
        });
    }

    {
        const CreateTopicsRequest = generated.create_topics_request.CreateTopicsRequest;
        const assignments = [_]CreateTopicsRequest.CreatableTopic.CreatableReplicaAssignment{
            .{ .partition_index = 0, .broker_ids = &[_]i32{ 1, 2 } },
        };
        const configs = [_]CreateTopicsRequest.CreatableTopic.CreatableTopicConfig{
            .{ .name = "cleanup.policy", .value = "compact" },
            .{ .name = "retention.ms", .value = "60000" },
        };
        const topics = [_]CreateTopicsRequest.CreatableTopic{
            .{
                .name = "golden-topic",
                .num_partitions = 3,
                .replication_factor = 1,
                .assignments = &assignments,
                .configs = &configs,
            },
        };
        const value = CreateTopicsRequest{
            .topics = &topics,
            .timeout_ms = 30000,
            .validate_only = true,
        };
        try expectGoldenRoundTrip(CreateTopicsRequest, value, 5, &[_]u8{
            0x02,
            0x0d,
            'g',
            'o',
            'l',
            'd',
            'e',
            'n',
            '-',
            't',
            'o',
            'p',
            'i',
            'c',
            0x00,
            0x00,
            0x00,
            0x03,
            0x00,
            0x01,
            0x02,
            0x00,
            0x00,
            0x00,
            0x00,
            0x03,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
            0x00,
            0x00,
            0x02,
            0x00,
            0x03,
            0x0f,
            'c',
            'l',
            'e',
            'a',
            'n',
            'u',
            'p',
            '.',
            'p',
            'o',
            'l',
            'i',
            'c',
            'y',
            0x08,
            'c',
            'o',
            'm',
            'p',
            'a',
            'c',
            't',
            0x00,
            0x0d,
            'r',
            'e',
            't',
            'e',
            'n',
            't',
            'i',
            'o',
            'n',
            '.',
            'm',
            's',
            0x06,
            '6',
            '0',
            '0',
            '0',
            '0',
            0x00,
            0x00,
            0x00,
            0x00,
            0x75,
            0x30,
            0x01,
            0x00,
        });
    }

    {
        const CreateTopicsResponse = generated.create_topics_response.CreateTopicsResponse;
        const topics = [_]CreateTopicsResponse.CreatableTopicResult{
            .{
                .name = "golden-topic",
                .topic_config_error_code = 40,
            },
        };
        const value = CreateTopicsResponse{
            .throttle_time_ms = 7,
            .topics = &topics,
        };
        try expectGoldenRoundTrip(CreateTopicsResponse, value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x02, 0x0d, 'g',  'o',
            'l',  'd',  'e',  'n',
            '-',  't',  'o',  'p',
            'i',  'c',  0x00, 0x00,
            0x00, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0x00,
            0x01, 0x00, 0x02, 0x00,
            0x28, 0x00,
        });

        const empty_configs = [_]CreateTopicsResponse.CreatableTopicResult.CreatableTopicConfigs{};
        const empty_topics = [_]CreateTopicsResponse.CreatableTopicResult{.{
            .name = "t",
            .configs = &empty_configs,
        }};
        const empty_value = CreateTopicsResponse{
            .topics = &empty_topics,
        };
        try expectGoldenRoundTrip(CreateTopicsResponse, empty_value, 5, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x02, 0x02, 't',  0x00,
            0x00, 0x00, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
            0x01, 0x00, 0x00,
        });
    }

    {
        const DescribeClientQuotasResponse = generated.describe_client_quotas_response.DescribeClientQuotasResponse;
        const null_entries = DescribeClientQuotasResponse{
            .error_code = 42,
            .error_message = "bad",
            .entries = null,
        };
        try expectGoldenRoundTrip(DescribeClientQuotasResponse, null_entries, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x2a, 0x00, 0x03,
            'b',  'a',  'd',  0xff,
            0xff, 0xff, 0xff,
        });
        try expectGoldenRoundTrip(DescribeClientQuotasResponse, null_entries, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x2a, 0x04, 'b',
            'a',  'd',  0x00, 0x00,
        });

        const empty_entries = [_]DescribeClientQuotasResponse.EntryData{};
        const empty_value = DescribeClientQuotasResponse{
            .entries = &empty_entries,
        };
        try expectGoldenRoundTrip(DescribeClientQuotasResponse, empty_value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01,
            0x00,
        });
    }

    {
        const DescribeClientQuotasRequest = generated.describe_client_quotas_request.DescribeClientQuotasRequest;
        const components = [_]DescribeClientQuotasRequest.ComponentData{
            .{
                .entity_type = "client-id",
                .match_type = 0,
                .match = "client-a",
            },
            .{
                .entity_type = "user",
                .match_type = 1,
                .match = null,
            },
        };
        const value = DescribeClientQuotasRequest{
            .components = &components,
            .strict = true,
        };
        try expectGoldenRoundTrip(DescribeClientQuotasRequest, value, 1, &[_]u8{
            0x03, 0x0a, 'c',  'l',
            'i',  'e',  'n',  't',
            '-',  'i',  'd',  0x00,
            0x09, 'c',  'l',  'i',
            'e',  'n',  't',  '-',
            'a',  0x00, 0x05, 'u',
            's',  'e',  'r',  0x01,
            0x00, 0x00, 0x01, 0x00,
        });
    }

    {
        const DescribeClientQuotasResponse = generated.describe_client_quotas_response.DescribeClientQuotasResponse;
        const EntityData = DescribeClientQuotasResponse.EntryData.EntityData;
        const ValueData = DescribeClientQuotasResponse.EntryData.ValueData;
        const entities = [_]EntityData{
            .{ .entity_type = "client-id", .entity_name = "client-a" },
            .{ .entity_type = "user", .entity_name = null },
        };
        const values = [_]ValueData{.{
            .key = "fetch",
            .value = 1.5,
        }};
        const entries = [_]DescribeClientQuotasResponse.EntryData{.{
            .entity = &entities,
            .values = &values,
        }};
        const value = DescribeClientQuotasResponse{
            .throttle_time_ms = 7,
            .entries = &entries,
        };
        try expectGoldenRoundTrip(DescribeClientQuotasResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x02,
            0x03, 0x0a, 'c',  'l',
            'i',  'e',  'n',  't',
            '-',  'i',  'd',  0x09,
            'c',  'l',  'i',  'e',
            'n',  't',  '-',  'a',
            0x00, 0x05, 'u',  's',
            'e',  'r',  0x00, 0x00,
            0x02, 0x06, 'f',  'e',
            't',  'c',  'h',  0x3f,
            0xf8, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        });
    }

    {
        const AlterClientQuotasRequest = generated.alter_client_quotas_request.AlterClientQuotasRequest;
        const EntityData = AlterClientQuotasRequest.EntryData.EntityData;
        const OpData = AlterClientQuotasRequest.EntryData.OpData;
        const entities = [_]EntityData{
            .{ .entity_type = "client-id", .entity_name = "client-a" },
            .{ .entity_type = "user", .entity_name = null },
        };
        const ops = [_]OpData{
            .{
                .key = "produce",
                .value = 100.0,
                .remove = false,
            },
            .{
                .key = "fetch",
                .value = 1.5,
                .remove = true,
            },
        };
        const entries = [_]AlterClientQuotasRequest.EntryData{.{
            .entity = &entities,
            .ops = &ops,
        }};
        const value = AlterClientQuotasRequest{
            .entries = &entries,
            .validate_only = true,
        };
        try expectGoldenRoundTrip(AlterClientQuotasRequest, value, 1, &[_]u8{
            0x02, 0x03, 0x0a, 'c',
            'l',  'i',  'e',  'n',
            't',  '-',  'i',  'd',
            0x09, 'c',  'l',  'i',
            'e',  'n',  't',  '-',
            'a',  0x00, 0x05, 'u',
            's',  'e',  'r',  0x00,
            0x00, 0x03, 0x08, 'p',
            'r',  'o',  'd',  'u',
            'c',  'e',  0x40, 0x59,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x06, 'f',  'e',  't',
            'c',  'h',  0x3f, 0xf8,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x00,
            0x00, 0x01, 0x00,
        });
    }

    {
        const AlterClientQuotasResponse = generated.alter_client_quotas_response.AlterClientQuotasResponse;
        const EntityData = AlterClientQuotasResponse.EntryData.EntityData;
        const entities = [_]EntityData{.{
            .entity_type = "client-id",
            .entity_name = "client-a",
        }};
        const entries = [_]AlterClientQuotasResponse.EntryData{.{
            .error_code = 42,
            .error_message = "invalid",
            .entity = &entities,
        }};
        const value = AlterClientQuotasResponse{
            .throttle_time_ms = 8,
            .entries = &entries,
        };
        try expectGoldenRoundTrip(AlterClientQuotasResponse, value, 1, &[_]u8{
            0x00, 0x00, 0x00, 0x08,
            0x02, 0x00, 0x2a, 0x08,
            'i',  'n',  'v',  'a',
            'l',  'i',  'd',  0x02,
            0x0a, 'c',  'l',  'i',
            'e',  'n',  't',  '-',
            'i',  'd',  0x09, 'c',
            'l',  'i',  'e',  'n',
            't',  '-',  'a',  0x00,
            0x00, 0x00,
        });
    }

    {
        const DescribeTopicPartitionsResponse = generated.describe_topic_partitions_response.DescribeTopicPartitionsResponse;
        const Topic = DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponseTopic;
        const Partition = Topic.DescribeTopicPartitionsResponsePartition;
        const replicas = [_]i32{7};

        const null_elr_partitions = [_]Partition{.{
            .partition_index = 0,
            .leader_id = 7,
            .leader_epoch = 4,
            .replica_nodes = &replicas,
            .isr_nodes = &replicas,
            .eligible_leader_replicas = null,
            .last_known_elr = null,
        }};
        const null_elr_topics = [_]Topic{.{
            .name = "t",
            .partitions = &null_elr_partitions,
        }};
        const null_elr_value = DescribeTopicPartitionsResponse{
            .topics = &null_elr_topics,
        };
        try expectGoldenRoundTrip(DescribeTopicPartitionsResponse, null_elr_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x02,
            't',  0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00,
            0x04, 0x02, 0x00, 0x00,
            0x00, 0x07, 0x02, 0x00,
            0x00, 0x00, 0x07, 0x00,
            0x00, 0x01, 0x00, 0x80,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        });

        const empty_elr = [_]i32{};
        const empty_elr_partitions = [_]Partition{.{
            .partition_index = 0,
            .leader_id = 7,
            .leader_epoch = 4,
            .replica_nodes = &replicas,
            .isr_nodes = &replicas,
            .eligible_leader_replicas = &empty_elr,
            .last_known_elr = &empty_elr,
        }};
        const empty_elr_topics = [_]Topic{.{
            .name = "t",
            .partitions = &empty_elr_partitions,
        }};
        const empty_elr_value = DescribeTopicPartitionsResponse{
            .topics = &empty_elr_topics,
        };
        try expectGoldenRoundTrip(DescribeTopicPartitionsResponse, empty_elr_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x02,
            't',  0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00,
            0x04, 0x02, 0x00, 0x00,
            0x00, 0x07, 0x02, 0x00,
            0x00, 0x00, 0x07, 0x01,
            0x01, 0x01, 0x00, 0x80,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        });
    }

    {
        const SnapshotResponse = generated.automq_get_partition_snapshot_response.AutomqGetPartitionSnapshotResponse;
        const PartitionSnapshot = SnapshotResponse.Topic.PartitionSnapshot;

        const null_partitions = [_]PartitionSnapshot{.{
            .partition_index = 2,
            .stream_metadata = null,
        }};
        const null_topics = [_]SnapshotResponse.Topic{.{ .partitions = &null_partitions }};
        const null_value = SnapshotResponse{
            .session_id = 1,
            .session_epoch = 2,
            .topics = &null_topics,
        };
        try expectGoldenRoundTrip(SnapshotResponse, null_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00,
            0x00, 0x02, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00,
        });

        const empty_stream_metadata = [_]PartitionSnapshot.StreamMetadata{};
        const empty_partitions = [_]PartitionSnapshot{.{
            .partition_index = 2,
            .stream_metadata = &empty_stream_metadata,
        }};
        const empty_topics = [_]SnapshotResponse.Topic{.{ .partitions = &empty_partitions }};
        const empty_value = SnapshotResponse{
            .session_id = 1,
            .session_epoch = 2,
            .topics = &empty_topics,
        };
        try expectGoldenRoundTrip(SnapshotResponse, empty_value, 0, &[_]u8{
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00,
            0x00, 0x02, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00,
            0x00,
        });
    }

    {
        const UpdateMetadataRequest = generated.update_metadata_request.UpdateMetadataRequest;
        const topic_id = [_]u8{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        };
        const partition_states = [_]generated.update_metadata_request.UpdateMetadataPartitionState{.{
            .partition_index = 1,
            .controller_epoch = 7,
            .leader = 2,
            .leader_epoch = 3,
            .isr = &[_]i32{2},
            .zk_version = 4,
            .replicas = &[_]i32{ 2, 3 },
            .offline_replicas = &[_]i32{3},
        }};
        const topic_states = [_]UpdateMetadataRequest.UpdateMetadataTopicState{.{
            .topic_name = "t",
            .topic_id = topic_id,
            .partition_states = &partition_states,
        }};
        const endpoints = [_]UpdateMetadataRequest.UpdateMetadataBroker.UpdateMetadataEndpoint{.{
            .port = 9092,
            .host = "host",
            .listener = "PLAINTEXT",
            .security_protocol = 0,
        }};
        const live_brokers = [_]UpdateMetadataRequest.UpdateMetadataBroker{.{
            .id = 2,
            .endpoints = &endpoints,
            .rack = "rack-a",
        }};
        const value = UpdateMetadataRequest{
            .controller_id = 2,
            .is_k_raft_controller = true,
            .metadata_type = 2,
            .controller_epoch = 7,
            .broker_epoch = 9,
            .topic_states = &topic_states,
            .live_brokers = &live_brokers,
        };
        try expectGoldenRoundTrip(UpdateMetadataRequest, value, 8, &[_]u8{
            0x00, 0x00, 0x00, 0x02,
            0x01, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x09, 0x02, 0x02, 't',
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
            0x02, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x03, 0x02, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00,
            0x00, 0x04, 0x03, 0x00,
            0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x02,
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x02, 0x02,
            0x00, 0x00, 0x23, 0x84,
            0x05, 'h',  'o',  's',
            't',  0x0a, 'P',  'L',
            'A',  'I',  'N',  'T',
            'E',  'X',  'T',  0x00,
            0x00, 0x00, 0x07, 'r',
            'a',  'c',  'k',  '-',
            'a',  0x00, 0x01, 0x00,
            0x01, 0x02,
        });
    }
}

fn expectDuplicateNodeEndpointsTagRejected(comptime Message: type) !void {
    const bytes = [_]u8{
        0x00, 0x00, 0x01, 0x02,
        0x00, 0x0b, 0x02, 0x00,
        0x00, 0x00, 0x07, 0x03,
        'n',  '1',  0x23, 0x84,
        0x00, 0x00, 0x0b, 0x02,
        0x00, 0x00, 0x00, 0x07,
        0x03, 'n',  '1',  0x23,
        0x84, 0x00,
    };
    var pos: usize = 0;
    try testing.expectError(error.DuplicateTaggedField, Message.deserialize(testing.allocator, &bytes, &pos, 1));
}

test "generated KRaft quorum responses reject duplicate node endpoint tags" {
    try expectDuplicateNodeEndpointsTagRejected(generated.vote_response.VoteResponse);
    try expectDuplicateNodeEndpointsTagRejected(generated.begin_quorum_epoch_response.BeginQuorumEpochResponse);
    try expectDuplicateNodeEndpointsTagRejected(generated.end_quorum_epoch_response.EndQuorumEpochResponse);
}

test "generated FetchSnapshotResponse rejects duplicate known tags" {
    {
        const duplicate_node_endpoints = [_]u8{
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
            0x00, 0x0b, 0x02, 0x00, 0x00, 0x00, 0x07, 0x03,
            'n',  '1',  0x23, 0x84, 0x00, 0x00, 0x0b, 0x02,
            0x00, 0x00, 0x00, 0x07, 0x03, 'n',  '1',  0x23,
            0x84, 0x00,
        };
        var pos: usize = 0;
        try testing.expectError(
            error.DuplicateTaggedField,
            generated.fetch_snapshot_response.FetchSnapshotResponse.deserialize(testing.allocator, &duplicate_node_endpoints, &pos, 1),
        );
    }

    {
        var duplicate_current_leader: [128]u8 = undefined;
        var pos: usize = 0;
        ser.writeI32(&duplicate_current_leader, &pos, 0); // throttle_time_ms
        ser.writeI16(&duplicate_current_leader, &pos, 0); // error_code
        ser.writeCompactArrayLen(&duplicate_current_leader, &pos, 1); // topics
        ser.writeCompactString(&duplicate_current_leader, &pos, "t");
        ser.writeCompactArrayLen(&duplicate_current_leader, &pos, 1); // partitions
        ser.writeI32(&duplicate_current_leader, &pos, 0); // index
        ser.writeI16(&duplicate_current_leader, &pos, 0); // error_code
        ser.writeI64(&duplicate_current_leader, &pos, 0); // snapshot end_offset
        ser.writeI32(&duplicate_current_leader, &pos, 0); // snapshot epoch
        ser.writeEmptyTaggedFields(&duplicate_current_leader, &pos); // SnapshotId tags
        ser.writeI64(&duplicate_current_leader, &pos, 0); // size
        ser.writeI64(&duplicate_current_leader, &pos, 0); // position
        ser.writeCompactBytes(&duplicate_current_leader, &pos, null);
        ser.writeUnsignedVarint(&duplicate_current_leader, &pos, 2); // duplicate partition tags
        inline for (0..2) |_| {
            ser.writeUnsignedVarint(&duplicate_current_leader, &pos, 0);
            ser.writeUnsignedVarint(&duplicate_current_leader, &pos, 9);
            ser.writeI32(&duplicate_current_leader, &pos, 7);
            ser.writeI32(&duplicate_current_leader, &pos, 4);
            ser.writeEmptyTaggedFields(&duplicate_current_leader, &pos);
        }
        ser.writeEmptyTaggedFields(&duplicate_current_leader, &pos); // topic tags
        ser.writeEmptyTaggedFields(&duplicate_current_leader, &pos); // response tags

        var read_pos: usize = 0;
        try testing.expectError(
            error.DuplicateTaggedField,
            generated.fetch_snapshot_response.FetchSnapshotResponse.deserialize(testing.allocator, duplicate_current_leader[0..pos], &read_pos, 1),
        );
    }
}

test "generated FetchSnapshotRequest rejects duplicate known tags" {
    {
        const duplicate_cluster_id = [_]u8{
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x02, 0x00, 0x02, 0x02, 'c',  0x00, 0x02,
            0x02, 'c',
        };
        var pos: usize = 0;
        try testing.expectError(
            error.DuplicateTaggedField,
            generated.fetch_snapshot_request.FetchSnapshotRequest.deserialize(testing.allocator, &duplicate_cluster_id, &pos, 1),
        );
    }

    {
        var duplicate_replica_directory: [160]u8 = undefined;
        var pos: usize = 0;
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // replica_id
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // max_bytes
        ser.writeCompactArrayLen(&duplicate_replica_directory, &pos, 1); // topics
        ser.writeCompactString(&duplicate_replica_directory, &pos, "t");
        ser.writeCompactArrayLen(&duplicate_replica_directory, &pos, 1); // partitions
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // partition
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // current_leader_epoch
        ser.writeI64(&duplicate_replica_directory, &pos, 0); // snapshot end_offset
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // snapshot epoch
        ser.writeEmptyTaggedFields(&duplicate_replica_directory, &pos); // SnapshotId tags
        ser.writeI64(&duplicate_replica_directory, &pos, 0); // position
        ser.writeUnsignedVarint(&duplicate_replica_directory, &pos, 2); // duplicate partition tags
        inline for (0..2) |_| {
            ser.writeUnsignedVarint(&duplicate_replica_directory, &pos, 0);
            ser.writeUnsignedVarint(&duplicate_replica_directory, &pos, 16);
            ser.writeUuid(&duplicate_replica_directory, &pos, .{
                0x00, 0x01, 0x02, 0x03,
                0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b,
                0x0c, 0x0d, 0x0e, 0x0f,
            });
        }
        ser.writeEmptyTaggedFields(&duplicate_replica_directory, &pos); // topic tags
        ser.writeEmptyTaggedFields(&duplicate_replica_directory, &pos); // request tags

        var read_pos: usize = 0;
        try testing.expectError(
            error.DuplicateTaggedField,
            generated.fetch_snapshot_request.FetchSnapshotRequest.deserialize(testing.allocator, duplicate_replica_directory[0..pos], &read_pos, 1),
        );
    }
}

test "generated FetchRequest rejects duplicate known tags" {
    {
        var duplicate_cluster_id: [128]u8 = undefined;
        var pos: usize = 0;
        ser.writeI32(&duplicate_cluster_id, &pos, 0); // max_wait_ms
        ser.writeI32(&duplicate_cluster_id, &pos, 0); // min_bytes
        ser.writeI32(&duplicate_cluster_id, &pos, 0); // max_bytes
        ser.writeI8(&duplicate_cluster_id, &pos, 0); // isolation_level
        ser.writeI32(&duplicate_cluster_id, &pos, 0); // session_id
        ser.writeI32(&duplicate_cluster_id, &pos, -1); // session_epoch
        ser.writeCompactArrayLen(&duplicate_cluster_id, &pos, 0); // topics
        ser.writeCompactArrayLen(&duplicate_cluster_id, &pos, 0); // forgotten_topics_data
        ser.writeCompactString(&duplicate_cluster_id, &pos, ""); // rack_id
        ser.writeUnsignedVarint(&duplicate_cluster_id, &pos, 2);
        inline for (0..2) |_| {
            ser.writeUnsignedVarint(&duplicate_cluster_id, &pos, 0);
            ser.writeUnsignedVarint(&duplicate_cluster_id, &pos, 2);
            ser.writeCompactString(&duplicate_cluster_id, &pos, "c");
        }

        var read_pos: usize = 0;
        try testing.expectError(
            error.DuplicateTaggedField,
            generated.fetch_request.FetchRequest.deserialize(testing.allocator, duplicate_cluster_id[0..pos], &read_pos, 17),
        );
    }

    {
        var duplicate_replica_state: [128]u8 = undefined;
        var pos: usize = 0;
        ser.writeI32(&duplicate_replica_state, &pos, 0); // max_wait_ms
        ser.writeI32(&duplicate_replica_state, &pos, 0); // min_bytes
        ser.writeI32(&duplicate_replica_state, &pos, 0); // max_bytes
        ser.writeI8(&duplicate_replica_state, &pos, 0); // isolation_level
        ser.writeI32(&duplicate_replica_state, &pos, 0); // session_id
        ser.writeI32(&duplicate_replica_state, &pos, -1); // session_epoch
        ser.writeCompactArrayLen(&duplicate_replica_state, &pos, 0); // topics
        ser.writeCompactArrayLen(&duplicate_replica_state, &pos, 0); // forgotten_topics_data
        ser.writeCompactString(&duplicate_replica_state, &pos, ""); // rack_id
        ser.writeUnsignedVarint(&duplicate_replica_state, &pos, 2);
        inline for (0..2) |_| {
            ser.writeUnsignedVarint(&duplicate_replica_state, &pos, 1);
            ser.writeUnsignedVarint(&duplicate_replica_state, &pos, 13);
            ser.writeI32(&duplicate_replica_state, &pos, 7);
            ser.writeI64(&duplicate_replica_state, &pos, 4);
            ser.writeEmptyTaggedFields(&duplicate_replica_state, &pos);
        }

        var read_pos: usize = 0;
        try testing.expectError(
            error.DuplicateTaggedField,
            generated.fetch_request.FetchRequest.deserialize(testing.allocator, duplicate_replica_state[0..pos], &read_pos, 17),
        );
    }

    {
        var duplicate_replica_directory: [256]u8 = undefined;
        var pos: usize = 0;
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // max_wait_ms
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // min_bytes
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // max_bytes
        ser.writeI8(&duplicate_replica_directory, &pos, 0); // isolation_level
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // session_id
        ser.writeI32(&duplicate_replica_directory, &pos, -1); // session_epoch
        ser.writeCompactArrayLen(&duplicate_replica_directory, &pos, 1); // topics
        ser.writeUuid(&duplicate_replica_directory, &pos, .{
            0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        });
        ser.writeCompactArrayLen(&duplicate_replica_directory, &pos, 1); // partitions
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // partition
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // current_leader_epoch
        ser.writeI64(&duplicate_replica_directory, &pos, 0); // fetch_offset
        ser.writeI32(&duplicate_replica_directory, &pos, -1); // last_fetched_epoch
        ser.writeI64(&duplicate_replica_directory, &pos, -1); // log_start_offset
        ser.writeI32(&duplicate_replica_directory, &pos, 0); // partition_max_bytes
        ser.writeUnsignedVarint(&duplicate_replica_directory, &pos, 2);
        inline for (0..2) |_| {
            ser.writeUnsignedVarint(&duplicate_replica_directory, &pos, 0);
            ser.writeUnsignedVarint(&duplicate_replica_directory, &pos, 16);
            ser.writeUuid(&duplicate_replica_directory, &pos, .{
                0x00, 0x01, 0x02, 0x03,
                0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b,
                0x0c, 0x0d, 0x0e, 0x0f,
            });
        }
        ser.writeEmptyTaggedFields(&duplicate_replica_directory, &pos); // topic tags
        ser.writeCompactArrayLen(&duplicate_replica_directory, &pos, 0); // forgotten_topics_data
        ser.writeCompactString(&duplicate_replica_directory, &pos, ""); // rack_id
        ser.writeEmptyTaggedFields(&duplicate_replica_directory, &pos); // request tags

        var read_pos: usize = 0;
        try testing.expectError(
            error.DuplicateTaggedField,
            generated.fetch_request.FetchRequest.deserialize(testing.allocator, duplicate_replica_directory[0..pos], &read_pos, 17),
        );
    }
}

test "generated BrokerHeartbeatRequest rejects duplicate offline log dir tags" {
    var duplicate_offline_log_dirs: [128]u8 = undefined;
    var pos: usize = 0;
    ser.writeI32(&duplicate_offline_log_dirs, &pos, 0); // broker_id
    ser.writeI64(&duplicate_offline_log_dirs, &pos, 0); // broker_epoch
    ser.writeI64(&duplicate_offline_log_dirs, &pos, 0); // current_metadata_offset
    ser.writeBool(&duplicate_offline_log_dirs, &pos, false);
    ser.writeBool(&duplicate_offline_log_dirs, &pos, false);
    ser.writeUnsignedVarint(&duplicate_offline_log_dirs, &pos, 2);
    inline for (0..2) |_| {
        ser.writeUnsignedVarint(&duplicate_offline_log_dirs, &pos, 0);
        ser.writeUnsignedVarint(&duplicate_offline_log_dirs, &pos, 17);
        ser.writeCompactArrayLen(&duplicate_offline_log_dirs, &pos, 1);
        ser.writeUuid(&duplicate_offline_log_dirs, &pos, .{
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f,
        });
    }

    var read_pos: usize = 0;
    try testing.expectError(
        error.DuplicateTaggedField,
        generated.broker_heartbeat_request.BrokerHeartbeatRequest.deserialize(testing.allocator, duplicate_offline_log_dirs[0..pos], &read_pos, 1),
    );
}

test "generated CreateTopicsResponse rejects duplicate topic config error tags" {
    var duplicate_topic_config_error: [128]u8 = undefined;
    var pos: usize = 0;
    ser.writeI32(&duplicate_topic_config_error, &pos, 0); // throttle_time_ms
    ser.writeCompactArrayLen(&duplicate_topic_config_error, &pos, 1); // topics
    ser.writeCompactString(&duplicate_topic_config_error, &pos, "t");
    ser.writeI16(&duplicate_topic_config_error, &pos, 0); // error_code
    ser.writeCompactString(&duplicate_topic_config_error, &pos, null); // error_message
    ser.writeI32(&duplicate_topic_config_error, &pos, -1); // num_partitions
    ser.writeI16(&duplicate_topic_config_error, &pos, -1); // replication_factor
    ser.writeCompactArrayLen(&duplicate_topic_config_error, &pos, null); // configs
    ser.writeUnsignedVarint(&duplicate_topic_config_error, &pos, 2);
    inline for (0..2) |_| {
        ser.writeUnsignedVarint(&duplicate_topic_config_error, &pos, 0);
        ser.writeUnsignedVarint(&duplicate_topic_config_error, &pos, 2);
        ser.writeI16(&duplicate_topic_config_error, &pos, 40);
    }
    ser.writeEmptyTaggedFields(&duplicate_topic_config_error, &pos); // response tags

    var read_pos: usize = 0;
    try testing.expectError(
        error.DuplicateTaggedField,
        generated.create_topics_response.CreateTopicsResponse.deserialize(testing.allocator, duplicate_topic_config_error[0..pos], &read_pos, 5),
    );
}

test "generated UpdateMetadataRequest rejects duplicate metadata type tags" {
    var duplicate_metadata_type: [128]u8 = undefined;
    var pos: usize = 0;
    ser.writeI32(&duplicate_metadata_type, &pos, 2); // controller_id
    ser.writeBool(&duplicate_metadata_type, &pos, true); // is_k_raft_controller
    ser.writeI32(&duplicate_metadata_type, &pos, 7); // controller_epoch
    ser.writeI64(&duplicate_metadata_type, &pos, 9); // broker_epoch
    ser.writeCompactArrayLen(&duplicate_metadata_type, &pos, 0); // topic_states
    ser.writeCompactArrayLen(&duplicate_metadata_type, &pos, 0); // live_brokers
    ser.writeUnsignedVarint(&duplicate_metadata_type, &pos, 2);
    inline for (0..2) |_| {
        ser.writeUnsignedVarint(&duplicate_metadata_type, &pos, 0);
        ser.writeUnsignedVarint(&duplicate_metadata_type, &pos, 1);
        ser.writeI8(&duplicate_metadata_type, &pos, 2);
    }

    var read_pos: usize = 0;
    try testing.expectError(
        error.DuplicateTaggedField,
        generated.update_metadata_request.UpdateMetadataRequest.deserialize(testing.allocator, duplicate_metadata_type[0..pos], &read_pos, 8),
    );
}

test "generated default messages round-trip across common protocol versions" {
    const messages = .{
        generated.add_offsets_to_txn_request.AddOffsetsToTxnRequest,
        generated.add_offsets_to_txn_response.AddOffsetsToTxnResponse,
        generated.add_partitions_to_txn_request.AddPartitionsToTxnRequest,
        generated.add_partitions_to_txn_response.AddPartitionsToTxnResponse,
        generated.add_raft_voter_request.AddRaftVoterRequest,
        generated.add_raft_voter_response.AddRaftVoterResponse,
        generated.allocate_producer_ids_request.AllocateProducerIdsRequest,
        generated.allocate_producer_ids_response.AllocateProducerIdsResponse,
        generated.alter_client_quotas_request.AlterClientQuotasRequest,
        generated.alter_client_quotas_response.AlterClientQuotasResponse,
        generated.alter_configs_request.AlterConfigsRequest,
        generated.alter_configs_response.AlterConfigsResponse,
        generated.alter_partition_reassignments_request.AlterPartitionReassignmentsRequest,
        generated.alter_partition_reassignments_response.AlterPartitionReassignmentsResponse,
        generated.alter_partition_request.AlterPartitionRequest,
        generated.alter_partition_response.AlterPartitionResponse,
        generated.alter_replica_log_dirs_request.AlterReplicaLogDirsRequest,
        generated.alter_replica_log_dirs_response.AlterReplicaLogDirsResponse,
        generated.alter_user_scram_credentials_request.AlterUserScramCredentialsRequest,
        generated.alter_user_scram_credentials_response.AlterUserScramCredentialsResponse,
        generated.api_versions_request.ApiVersionsRequest,
        generated.api_versions_response.ApiVersionsResponse,
        generated.assign_replicas_to_dirs_request.AssignReplicasToDirsRequest,
        generated.assign_replicas_to_dirs_response.AssignReplicasToDirsResponse,
        generated.automq_get_nodes_request.AutomqGetNodesRequest,
        generated.automq_get_nodes_response.AutomqGetNodesResponse,
        generated.automq_get_partition_snapshot_request.AutomqGetPartitionSnapshotRequest,
        generated.automq_get_partition_snapshot_response.AutomqGetPartitionSnapshotResponse,
        generated.automq_register_node_request.AutomqRegisterNodeRequest,
        generated.automq_register_node_response.AutomqRegisterNodeResponse,
        generated.automq_update_group_request.AutomqUpdateGroupRequest,
        generated.automq_update_group_response.AutomqUpdateGroupResponse,
        generated.automq_zone_router_request.AutomqZoneRouterRequest,
        generated.automq_zone_router_response.AutomqZoneRouterResponse,
        generated.begin_quorum_epoch_request.BeginQuorumEpochRequest,
        generated.begin_quorum_epoch_response.BeginQuorumEpochResponse,
        generated.broker_heartbeat_request.BrokerHeartbeatRequest,
        generated.broker_heartbeat_response.BrokerHeartbeatResponse,
        generated.broker_registration_request.BrokerRegistrationRequest,
        generated.broker_registration_response.BrokerRegistrationResponse,
        generated.close_streams_request.CloseStreamsRequest,
        generated.close_streams_response.CloseStreamsResponse,
        generated.commit_stream_object_request.CommitStreamObjectRequest,
        generated.commit_stream_object_response.CommitStreamObjectResponse,
        generated.commit_stream_set_object_request.CommitStreamSetObjectRequest,
        generated.commit_stream_set_object_response.CommitStreamSetObjectResponse,
        generated.consumer_group_describe_request.ConsumerGroupDescribeRequest,
        generated.consumer_group_describe_response.ConsumerGroupDescribeResponse,
        generated.consumer_group_heartbeat_request.ConsumerGroupHeartbeatRequest,
        generated.consumer_group_heartbeat_response.ConsumerGroupHeartbeatResponse,
        generated.consumer_protocol_assignment.ConsumerProtocolAssignment,
        generated.consumer_protocol_subscription.ConsumerProtocolSubscription,
        generated.controlled_shutdown_request.ControlledShutdownRequest,
        generated.controlled_shutdown_response.ControlledShutdownResponse,
        generated.controller_registration_request.ControllerRegistrationRequest,
        generated.controller_registration_response.ControllerRegistrationResponse,
        generated.create_acls_request.CreateAclsRequest,
        generated.create_acls_response.CreateAclsResponse,
        generated.create_delegation_token_request.CreateDelegationTokenRequest,
        generated.create_delegation_token_response.CreateDelegationTokenResponse,
        generated.create_partitions_request.CreatePartitionsRequest,
        generated.create_partitions_response.CreatePartitionsResponse,
        generated.create_streams_request.CreateStreamsRequest,
        generated.create_streams_response.CreateStreamsResponse,
        generated.create_topics_request.CreateTopicsRequest,
        generated.create_topics_response.CreateTopicsResponse,
        generated.default_principal_data.DefaultPrincipalData,
        generated.delete_acls_request.DeleteAclsRequest,
        generated.delete_acls_response.DeleteAclsResponse,
        generated.delete_groups_request.DeleteGroupsRequest,
        generated.delete_groups_response.DeleteGroupsResponse,
        generated.delete_k_vs_request.DeleteKVsRequest,
        generated.delete_k_vs_response.DeleteKVsResponse,
        generated.delete_records_request.DeleteRecordsRequest,
        generated.delete_records_response.DeleteRecordsResponse,
        generated.delete_share_group_state_request.DeleteShareGroupStateRequest,
        generated.delete_share_group_state_response.DeleteShareGroupStateResponse,
        generated.delete_streams_request.DeleteStreamsRequest,
        generated.delete_streams_response.DeleteStreamsResponse,
        generated.delete_topics_request.DeleteTopicsRequest,
        generated.delete_topics_response.DeleteTopicsResponse,
        generated.describe_acls_request.DescribeAclsRequest,
        generated.describe_acls_response.DescribeAclsResponse,
        generated.describe_client_quotas_request.DescribeClientQuotasRequest,
        generated.describe_client_quotas_response.DescribeClientQuotasResponse,
        generated.describe_cluster_request.DescribeClusterRequest,
        generated.describe_cluster_response.DescribeClusterResponse,
        generated.describe_configs_request.DescribeConfigsRequest,
        generated.describe_configs_response.DescribeConfigsResponse,
        generated.describe_delegation_token_request.DescribeDelegationTokenRequest,
        generated.describe_delegation_token_response.DescribeDelegationTokenResponse,
        generated.describe_groups_request.DescribeGroupsRequest,
        generated.describe_groups_response.DescribeGroupsResponse,
        generated.describe_license_request.DescribeLicenseRequest,
        generated.describe_license_response.DescribeLicenseResponse,
        generated.describe_log_dirs_request.DescribeLogDirsRequest,
        generated.describe_log_dirs_response.DescribeLogDirsResponse,
        generated.describe_producers_request.DescribeProducersRequest,
        generated.describe_producers_response.DescribeProducersResponse,
        generated.describe_quorum_request.DescribeQuorumRequest,
        generated.describe_quorum_response.DescribeQuorumResponse,
        generated.describe_streams_request.DescribeStreamsRequest,
        generated.describe_streams_response.DescribeStreamsResponse,
        generated.describe_topic_partitions_request.DescribeTopicPartitionsRequest,
        generated.describe_topic_partitions_response.DescribeTopicPartitionsResponse,
        generated.describe_transactions_request.DescribeTransactionsRequest,
        generated.describe_transactions_response.DescribeTransactionsResponse,
        generated.describe_user_scram_credentials_request.DescribeUserScramCredentialsRequest,
        generated.describe_user_scram_credentials_response.DescribeUserScramCredentialsResponse,
        generated.elect_leaders_request.ElectLeadersRequest,
        generated.elect_leaders_response.ElectLeadersResponse,
        generated.end_quorum_epoch_request.EndQuorumEpochRequest,
        generated.end_quorum_epoch_response.EndQuorumEpochResponse,
        generated.end_txn_request.EndTxnRequest,
        generated.end_txn_response.EndTxnResponse,
        generated.envelope_request.EnvelopeRequest,
        generated.envelope_response.EnvelopeResponse,
        generated.expire_delegation_token_request.ExpireDelegationTokenRequest,
        generated.expire_delegation_token_response.ExpireDelegationTokenResponse,
        generated.export_cluster_manifest_request.ExportClusterManifestRequest,
        generated.export_cluster_manifest_response.ExportClusterManifestResponse,
        generated.fetch_request.FetchRequest,
        generated.fetch_response.FetchResponse,
        generated.fetch_snapshot_request.FetchSnapshotRequest,
        generated.fetch_snapshot_response.FetchSnapshotResponse,
        generated.find_coordinator_request.FindCoordinatorRequest,
        generated.find_coordinator_response.FindCoordinatorResponse,
        generated.get_k_vs_request.GetKVsRequest,
        generated.get_k_vs_response.GetKVsResponse,
        generated.get_next_node_id_request.GetNextNodeIdRequest,
        generated.get_next_node_id_response.GetNextNodeIdResponse,
        generated.get_opening_streams_request.GetOpeningStreamsRequest,
        generated.get_opening_streams_response.GetOpeningStreamsResponse,
        generated.get_telemetry_subscriptions_request.GetTelemetrySubscriptionsRequest,
        generated.get_telemetry_subscriptions_response.GetTelemetrySubscriptionsResponse,
        generated.heartbeat_request.HeartbeatRequest,
        generated.heartbeat_response.HeartbeatResponse,
        generated.incremental_alter_configs_request.IncrementalAlterConfigsRequest,
        generated.incremental_alter_configs_response.IncrementalAlterConfigsResponse,
        generated.init_producer_id_request.InitProducerIdRequest,
        generated.init_producer_id_response.InitProducerIdResponse,
        generated.initialize_share_group_state_request.InitializeShareGroupStateRequest,
        generated.initialize_share_group_state_response.InitializeShareGroupStateResponse,
        generated.join_group_request.JoinGroupRequest,
        generated.join_group_response.JoinGroupResponse,
        generated.k_raft_version_record.KRaftVersionRecord,
        generated.leader_and_isr_request.LeaderAndIsrRequest,
        generated.leader_and_isr_response.LeaderAndIsrResponse,
        generated.leader_change_message.LeaderChangeMessage,
        generated.leave_group_request.LeaveGroupRequest,
        generated.leave_group_response.LeaveGroupResponse,
        generated.list_client_metrics_resources_request.ListClientMetricsResourcesRequest,
        generated.list_client_metrics_resources_response.ListClientMetricsResourcesResponse,
        generated.list_groups_request.ListGroupsRequest,
        generated.list_groups_response.ListGroupsResponse,
        generated.list_offsets_request.ListOffsetsRequest,
        generated.list_offsets_response.ListOffsetsResponse,
        generated.list_partition_reassignments_request.ListPartitionReassignmentsRequest,
        generated.list_partition_reassignments_response.ListPartitionReassignmentsResponse,
        generated.list_transactions_request.ListTransactionsRequest,
        generated.list_transactions_response.ListTransactionsResponse,
        generated.metadata_request.MetadataRequest,
        generated.metadata_response.MetadataResponse,
        generated.offset_commit_request.OffsetCommitRequest,
        generated.offset_commit_response.OffsetCommitResponse,
        generated.offset_delete_request.OffsetDeleteRequest,
        generated.offset_delete_response.OffsetDeleteResponse,
        generated.offset_fetch_request.OffsetFetchRequest,
        generated.offset_fetch_response.OffsetFetchResponse,
        generated.offset_for_leader_epoch_request.OffsetForLeaderEpochRequest,
        generated.offset_for_leader_epoch_response.OffsetForLeaderEpochResponse,
        generated.open_streams_request.OpenStreamsRequest,
        generated.open_streams_response.OpenStreamsResponse,
        generated.prepare_s3_object_request.PrepareS3ObjectRequest,
        generated.prepare_s3_object_response.PrepareS3ObjectResponse,
        generated.produce_request.ProduceRequest,
        generated.produce_response.ProduceResponse,
        generated.push_telemetry_request.PushTelemetryRequest,
        generated.push_telemetry_response.PushTelemetryResponse,
        generated.put_k_vs_request.PutKVsRequest,
        generated.put_k_vs_response.PutKVsResponse,
        generated.read_share_group_state_request.ReadShareGroupStateRequest,
        generated.read_share_group_state_response.ReadShareGroupStateResponse,
        generated.read_share_group_state_summary_request.ReadShareGroupStateSummaryRequest,
        generated.read_share_group_state_summary_response.ReadShareGroupStateSummaryResponse,
        generated.remove_raft_voter_request.RemoveRaftVoterRequest,
        generated.remove_raft_voter_response.RemoveRaftVoterResponse,
        generated.renew_delegation_token_request.RenewDelegationTokenRequest,
        generated.renew_delegation_token_response.RenewDelegationTokenResponse,
        generated.request_header.RequestHeader,
        generated.response_header.ResponseHeader,
        generated.sasl_authenticate_request.SaslAuthenticateRequest,
        generated.sasl_authenticate_response.SaslAuthenticateResponse,
        generated.sasl_handshake_request.SaslHandshakeRequest,
        generated.sasl_handshake_response.SaslHandshakeResponse,
        generated.share_acknowledge_request.ShareAcknowledgeRequest,
        generated.share_acknowledge_response.ShareAcknowledgeResponse,
        generated.share_fetch_request.ShareFetchRequest,
        generated.share_fetch_response.ShareFetchResponse,
        generated.share_group_describe_request.ShareGroupDescribeRequest,
        generated.share_group_describe_response.ShareGroupDescribeResponse,
        generated.share_group_heartbeat_request.ShareGroupHeartbeatRequest,
        generated.share_group_heartbeat_response.ShareGroupHeartbeatResponse,
        generated.snapshot_footer_record.SnapshotFooterRecord,
        generated.snapshot_header_record.SnapshotHeaderRecord,
        generated.stop_replica_request.StopReplicaRequest,
        generated.stop_replica_response.StopReplicaResponse,
        generated.sync_group_request.SyncGroupRequest,
        generated.sync_group_response.SyncGroupResponse,
        generated.trim_streams_request.TrimStreamsRequest,
        generated.trim_streams_response.TrimStreamsResponse,
        generated.txn_offset_commit_request.TxnOffsetCommitRequest,
        generated.txn_offset_commit_response.TxnOffsetCommitResponse,
        generated.unregister_broker_request.UnregisterBrokerRequest,
        generated.unregister_broker_response.UnregisterBrokerResponse,
        generated.update_features_request.UpdateFeaturesRequest,
        generated.update_features_response.UpdateFeaturesResponse,
        generated.update_license_request.UpdateLicenseRequest,
        generated.update_license_response.UpdateLicenseResponse,
        generated.update_metadata_request.UpdateMetadataRequest,
        generated.update_metadata_response.UpdateMetadataResponse,
        generated.update_raft_voter_request.UpdateRaftVoterRequest,
        generated.update_raft_voter_response.UpdateRaftVoterResponse,
        generated.vote_request.VoteRequest,
        generated.vote_response.VoteResponse,
        generated.voters_record.VotersRecord,
        generated.write_share_group_state_request.WriteShareGroupStateRequest,
        generated.write_share_group_state_response.WriteShareGroupStateResponse,
        generated.write_txn_markers_request.WriteTxnMarkersRequest,
        generated.write_txn_markers_response.WriteTxnMarkersResponse,
    };

    inline for (messages) |Message| {
        try roundTripDefaultVersions(Message);
    }
}

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
        const partitions = [_]PartitionData{.{
            .partition_index = 2,
            .error_code = 6,
            .high_watermark = 123,
            .last_stable_offset = 100,
            .log_start_offset = 5,
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
        const ApiVersionsResponse = generated.api_versions_response.ApiVersionsResponse;
        const api_keys = [_]ApiVersionsResponse.ApiVersion{
            .{ .api_key = 0, .min_version = 0, .max_version = 11 },
            .{ .api_key = 3, .min_version = 0, .max_version = 12 },
        };
        const value = ApiVersionsResponse{
            .error_code = 0,
            .api_keys = &api_keys,
            .throttle_time_ms = 42,
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
            0x2a, 0x00,
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
            0xff, 0xff, 0xff, 0x01,
            0x01, 0x00, 0x02, 0x00,
            0x28, 0x00,
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
    ser.writeCompactArrayLen(&duplicate_topic_config_error, &pos, 0); // configs
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

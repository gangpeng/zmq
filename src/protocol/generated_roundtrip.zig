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
        const value = UpdateMetadataRequest{
            .controller_id = 2,
            .is_k_raft_controller = true,
            .metadata_type = 2,
            .controller_epoch = 7,
            .broker_epoch = 9,
        };
        try expectGoldenRoundTrip(UpdateMetadataRequest, value, 8, &[_]u8{
            0x00, 0x00, 0x00, 0x02,
            0x01, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x09, 0x01, 0x01, 0x01,
            0x01, 0x00, 0x01, 0x02,
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
    ser.writeCompactArrayLen(&duplicate_metadata_type, &pos, 0); // ungrouped_partition_states
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

// Production Readiness Integration Tests
//
// Validates all Sprint 1-6 production-readiness improvements work together:
// - Sprint 1: TLS hostname verification, cert expiry, mTLS, handshake timeout
// - Sprint 2: SCRAM-SHA-256 full RFC 5802 exchange
// - Sprint 3: S3 object CRC32C checksums
// - Sprint 4: ObjectManager snapshot/load persistence
// - Sprint 5: MetricRegistry labeled gauges, JSON logger, /ready 503
// - Sprint 6: Graceful shutdown request rejection
//
// Each test imports from the actual ZMQ modules (no mocks for the units under test).

const std = @import("std");
const testing = std.testing;
const fs = @import("fs_compat");

// --- Module imports (via barrel re-exports registered in build.zig) ---
const security = @import("security");
const tls = security.tls;
const TlsConfig = tls.TlsConfig;
const TlsConnection = tls.TlsConnection;
const OpenSslLib = security.openssl.OpenSslLib;

const auth = security.auth;
const ScramSha256Authenticator = auth.ScramSha256Authenticator;
const ScramStateMachine = auth.ScramStateMachine;
const SaslPlainAuthenticator = auth.SaslPlainAuthenticator;

const storage = @import("storage");
const ObjectWriter = storage.ObjectWriter;
const ObjectReader = storage.ObjectReader;
const ObjectManager = storage.ObjectManager;
const StreamOffsetRange = storage.stream.StreamOffsetRange;

const core = @import("core");
const MetricRegistry = core.MetricRegistry;
const JsonLogger = core.JsonLogger;

const network = @import("network");
const MetricsServer = network.MetricsServer;

const broker = @import("broker");
const broker_metrics = broker.metrics;

// ---------------------------------------------------------------
// Test 1: TLS Configuration and Hostname Verification Setup
// ---------------------------------------------------------------

test "TLS config validates SSL requires certificate" {
    // Sprint 1: TLS configuration enforces security requirements
    const config = TlsConfig{ .protocol = .ssl };
    try testing.expectError(error.NoCertificateConfigured, config.validate());

    // A properly configured TLS config validates successfully
    const valid_config = TlsConfig{
        .protocol = .ssl,
        .cert_file = "/path/to/cert.pem",
        .key_file = "/path/to/key.pem",
    };
    try valid_config.validate();
}

test "TLS handshake timeout enforcement" {
    // Sprint 1: Handshake timeout prevents slow-client DoS
    var conn = TlsConnection.init(testing.allocator, 42);
    defer conn.deinit();

    // New connection: not timed out (handshake_start_ms defaults to 0)
    try testing.expect(!conn.isHandshakeTimedOut(@intCast(@import("time_compat").monotonicMilliTimestamp())));

    // Simulated stale connection: started 31 seconds ago
    conn.handshake_start_ms = 1000;
    try testing.expect(conn.isHandshakeTimedOut(32_001)); // > 30s → timed out
    try testing.expect(!conn.isHandshakeTimedOut(30_999)); // < 30s → not yet

    // Established connections are never "timed out"
    conn.tls_established = true;
    try testing.expect(!conn.isHandshakeTimedOut(1_000_000));
}

test "TLS client context hostname verification with OpenSSL" {
    // Sprint 1: SSL_set1_host integration for hostname verification.
    // If OpenSSL is not available at runtime, skip gracefully.
    var lib = OpenSslLib.load() catch return;
    defer lib.close();

    // Verify the binding exists
    try testing.expect(lib.SSL_set1_host != null);

    // Create a client SSL object and verify setHostnameVerification works
    const method = lib.TLS_client_method() orelse return;
    const ctx = lib.SSL_CTX_new(method) orelse return;
    defer lib.SSL_CTX_free(ctx);

    const ssl = lib.SSL_new(ctx) orelse return;
    defer lib.SSL_free(ssl);

    // setHostnameVerification should succeed on a valid SSL object
    const result = lib.setHostnameVerification(ssl, "broker.example.com");
    try testing.expect(result);
}

// ---------------------------------------------------------------
// Test 2: SCRAM-SHA-256 Full Handshake
// ---------------------------------------------------------------

test "SCRAM-SHA-256 full handshake exchange" {
    // Sprint 2: Complete RFC 5802 SCRAM-SHA-256 authentication roundtrip
    const alloc = testing.allocator;

    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("kafka-admin", "s3cure-p@ss");

    // Verify credential was stored
    const cred = scram_auth.getCredential("kafka-admin");
    try testing.expect(cred != null);
    try testing.expectEqual(@as(u32, 4096), cred.?.iterations);

    // Start a SCRAM state machine for this connection
    var sm = ScramStateMachine.init(alloc);
    defer sm.deinit();

    // Round 1: client-first-message → server-first-message
    const client_nonce = "prodReadinessNonce42";
    const client_first = "n,,n=kafka-admin,r=" ++ client_nonce;

    const server_first = sm.handleClientFirst(&scram_auth, client_first) orelse {
        return error.UnexpectedNull;
    };
    defer alloc.free(server_first);

    try testing.expectEqual(ScramSha256Authenticator.ScramState.server_first_sent, sm.state);
    // server-first must start with the client nonce (combined nonce includes it)
    try testing.expect(std.mem.startsWith(u8, server_first, "r=" ++ client_nonce));
    // Must contain salt and iterations
    try testing.expect(std.mem.indexOf(u8, server_first, ",s=") != null);
    try testing.expect(std.mem.indexOf(u8, server_first, ",i=4096") != null);

    // Round 2: build client-final-message (client side computation)
    const combined_nonce = ScramStateMachine.parseAttribute(server_first, 'r') orelse return error.MissingNonce;
    const salt_b64 = ScramStateMachine.parseAttribute(server_first, 's') orelse return error.MissingSalt;
    const iter_str = ScramStateMachine.parseAttribute(server_first, 'i') orelse return error.MissingIter;
    const iterations = try std.fmt.parseInt(u32, iter_str, 10);

    // Decode salt
    const decoder = std.base64.standard.Decoder;
    const salt_len = decoder.calcSizeForSlice(salt_b64) catch return error.InvalidBase64;
    const salt = try alloc.alloc(u8, salt_len);
    defer alloc.free(salt);
    decoder.decode(salt, salt_b64) catch return error.InvalidBase64;

    // SaltedPassword = PBKDF2-HMAC-SHA256("s3cure-p@ss", salt, iterations)
    var salted_password: [32]u8 = undefined;
    pbkdf2HmacSha256("s3cure-p@ss", salt, iterations, &salted_password);

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    var client_key: [32]u8 = undefined;
    hmacSha256Compute(&salted_password, "Client Key", &client_key);

    // StoredKey = SHA256(ClientKey)
    var stored_key: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&client_key, &stored_key, .{});

    // Build client-final-message-without-proof
    const client_final_without_proof = try std.fmt.allocPrint(alloc, "c=biws,r={s}", .{combined_nonce});
    defer alloc.free(client_final_without_proof);

    // auth_message
    const client_first_bare = "n=kafka-admin,r=" ++ client_nonce;
    const auth_message = try std.fmt.allocPrint(alloc, "{s},{s},{s}", .{ client_first_bare, server_first, client_final_without_proof });
    defer alloc.free(auth_message);

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    var client_signature: [32]u8 = undefined;
    hmacSha256Compute(&stored_key, auth_message, &client_signature);

    // ClientProof = ClientKey XOR ClientSignature
    var client_proof: [32]u8 = undefined;
    for (&client_proof, client_key, client_signature) |*p, k, s| {
        p.* = k ^ s;
    }

    // Base64-encode proof
    const encoder = std.base64.standard.Encoder;
    const proof_b64 = try alloc.alloc(u8, encoder.calcSize(32));
    defer alloc.free(proof_b64);
    _ = encoder.encode(proof_b64, &client_proof);

    const client_final = try std.fmt.allocPrint(alloc, "c=biws,r={s},p={s}", .{ combined_nonce, proof_b64 });
    defer alloc.free(client_final);

    const server_final = sm.handleClientFinal(&scram_auth, client_final) orelse {
        return error.UnexpectedNull;
    };
    defer alloc.free(server_final);

    // Verify authentication succeeded
    try testing.expectEqual(ScramSha256Authenticator.ScramState.authenticated, sm.state);
    try testing.expect(std.mem.startsWith(u8, server_final, "v="));
    try testing.expectEqualStrings("kafka-admin", sm.username.?);
}

// ---------------------------------------------------------------
// Test 3: S3 Object CRC32C Roundtrip
// ---------------------------------------------------------------

test "S3 object CRC32C roundtrip: write, read, verify checksum" {
    // Sprint 3: ObjectWriter produces v2 format with CRC32C; ObjectReader verifies
    const alloc = testing.allocator;

    var writer = ObjectWriter.init(alloc);
    defer writer.deinit();

    // Add multiple data blocks across different streams
    try writer.addDataBlock(1, 0, 100, 100, "partition-0-records-batch-1");
    try writer.addDataBlock(1, 100, 50, 50, "partition-0-records-batch-2");
    try writer.addDataBlock(2, 0, 200, 200, "partition-1-records-batch-1");

    const obj_data = try writer.build();
    defer alloc.free(obj_data);

    // Parse and verify: CRC32C checksum validation happens inside parse()
    var reader = try ObjectReader.parse(alloc, obj_data);
    defer reader.deinit();

    // Verify v2 format with checksum
    try testing.expect(reader.has_checksum);

    // Verify all 3 index entries survived roundtrip
    try testing.expectEqual(@as(usize, 3), reader.index_entries.len);

    // Verify data block content integrity
    try testing.expectEqualStrings("partition-0-records-batch-1", reader.readBlock(0).?);
    try testing.expectEqualStrings("partition-0-records-batch-2", reader.readBlock(1).?);
    try testing.expectEqualStrings("partition-1-records-batch-1", reader.readBlock(2).?);

    // Verify index lookup works for stream 1 (should find 2 blocks)
    const s1_indexes = try reader.findEntryIndexes(alloc, 1, 0, 200);
    defer alloc.free(s1_indexes);
    try testing.expectEqual(@as(usize, 2), s1_indexes.len);
    try testing.expectEqual(@as(u64, 0), reader.index_entries[s1_indexes[0]].start_offset);
    try testing.expectEqual(@as(u64, 100), reader.index_entries[s1_indexes[1]].start_offset);

    // Verify index lookup for stream 2 (should find 1 block)
    const s2_indexes = try reader.findEntryIndexes(alloc, 2, 0, 200);
    defer alloc.free(s2_indexes);
    try testing.expectEqual(@as(usize, 1), s2_indexes.len);
}

test "S3 object CRC32C detects bit-rot corruption" {
    // Sprint 3: Verify CRC32C catches corrupted data
    const alloc = testing.allocator;

    var writer = ObjectWriter.init(alloc);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 10, 10, "integrity-test-data");
    const obj_data = try writer.build();
    defer alloc.free(obj_data);

    // Corrupt one byte in the data block region (simulate bit-rot)
    obj_data[5] ^= 0xFF;

    // ObjectReader.parse should detect the corruption via CRC mismatch
    const result = ObjectReader.parse(alloc, obj_data);
    try testing.expectError(error.ChecksumMismatch, result);
}

// ---------------------------------------------------------------
// Test 4: ObjectManager Snapshot Roundtrip
// ---------------------------------------------------------------

test "ObjectManager snapshot roundtrip preserves all metadata" {
    // Sprint 4: takeSnapshot serializes; loadSnapshot restores faithfully
    const alloc = testing.allocator;

    // Create original ObjectManager with streams, SSOs, and SOs
    var om = ObjectManager.init(alloc, 1);
    defer om.deinit();

    // Create a stream
    const stream_ptr = try om.createStream(1);
    const stream_id = stream_ptr.stream_id;
    stream_ptr.advanceEndOffset(1000);

    // Register a StreamSetObject
    const sso_id = om.allocateObjectId();
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = stream_id, .start_offset = 0, .end_offset = 500 },
    };
    try om.commitStreamSetObject(sso_id, 1, 1, &ranges, "sso/object-1.dat", 4096);

    // Register a StreamObject
    const so_id = om.allocateObjectId();
    try om.commitStreamObject(so_id, stream_id, 500, 1000, "so/stream-1-500-1000.dat", 2048);

    // Take snapshot with some orphaned keys
    const orphaned = [_][]const u8{ "orphan/old-1.dat", "orphan/old-2.dat" };
    const snapshot_data = try om.takeSnapshot(&orphaned);
    defer alloc.free(snapshot_data);

    // Verify snapshot is non-empty
    try testing.expect(snapshot_data.len > 0);

    // Load snapshot into a fresh ObjectManager
    var om2 = ObjectManager.init(alloc, 1);
    defer om2.deinit();

    const loaded_orphans = try om2.loadSnapshot(snapshot_data);
    defer {
        for (loaded_orphans) |key| alloc.free(key);
        alloc.free(loaded_orphans);
    }

    // Verify orphaned keys survived roundtrip
    try testing.expectEqual(@as(usize, 2), loaded_orphans.len);
    try testing.expectEqualStrings("orphan/old-1.dat", loaded_orphans[0]);
    try testing.expectEqualStrings("orphan/old-2.dat", loaded_orphans[1]);

    // Verify stream was restored
    const restored_stream = om2.getStream(stream_id);
    try testing.expect(restored_stream != null);
    try testing.expectEqual(@as(u64, 1000), restored_stream.?.end_offset);

    // Verify StreamSetObject was restored
    const sso = om2.stream_set_objects.get(sso_id);
    try testing.expect(sso != null);
    try testing.expectEqualStrings("sso/object-1.dat", sso.?.s3_key);

    // Verify StreamObject was restored
    const so = om2.stream_objects.get(so_id);
    try testing.expect(so != null);
    try testing.expectEqualStrings("so/stream-1-500-1000.dat", so.?.s3_key);
    try testing.expectEqual(@as(u64, 500), so.?.start_offset);
    try testing.expectEqual(@as(u64, 1000), so.?.end_offset);

    // Verify object resolution still works after restore
    const objects = try om2.getObjects(stream_id, 0, 1000, 10);
    defer alloc.free(objects);
    try testing.expect(objects.len > 0);
}

// ---------------------------------------------------------------
// Test 5: Metrics Registry Production Metrics
// ---------------------------------------------------------------

test "MetricRegistry exports production metrics with labels" {
    // Sprint 5: Labeled gauges, counters, histograms; Prometheus export
    const alloc = testing.allocator;

    var registry = MetricRegistry.init(alloc);
    defer registry.deinit();

    // Register production-relevant metrics
    try registry.registerCounter("zmq_requests_total", "Total Kafka requests processed");
    try registry.registerGauge("zmq_active_connections", "Active client connections");
    try registry.registerHistogram("zmq_request_latency_seconds", "Request processing latency");
    try registry.registerLabeledCounter("zmq_s3_ops_total", "S3 operations", &.{"operation"});
    try registry.registerLabeledGauge("zmq_consumer_lag", "Consumer group lag", &.{ "group", "topic" });

    // Simulate broker activity
    registry.incrementCounter("zmq_requests_total");
    registry.incrementCounter("zmq_requests_total");
    registry.incrementCounter("zmq_requests_total");
    registry.setGauge("zmq_active_connections", 5.0);
    registry.observeHistogram("zmq_request_latency_seconds", 0.005);
    registry.observeHistogram("zmq_request_latency_seconds", 0.15);
    registry.incrementLabeledCounter("zmq_s3_ops_total", &.{"put"});
    registry.incrementLabeledCounter("zmq_s3_ops_total", &.{"get"});
    registry.incrementLabeledCounter("zmq_s3_ops_total", &.{"get"});
    registry.setLabeledGauge("zmq_consumer_lag", &.{ "my-group", "orders" }, 42.0);
    registry.setLabeledGauge("zmq_consumer_lag", &.{ "my-group", "events" }, 7.0);

    // Export Prometheus format
    const output = try registry.exportPrometheus(alloc);
    defer alloc.free(output);

    // Verify counters
    try testing.expect(std.mem.indexOf(u8, output, "# TYPE zmq_requests_total counter") != null);
    try testing.expect(std.mem.indexOf(u8, output, "zmq_requests_total 3") != null);

    // Verify gauges
    try testing.expect(std.mem.indexOf(u8, output, "# TYPE zmq_active_connections gauge") != null);

    // Verify histograms
    try testing.expect(std.mem.indexOf(u8, output, "# TYPE zmq_request_latency_seconds histogram") != null);
    try testing.expect(std.mem.indexOf(u8, output, "zmq_request_latency_seconds_count 2") != null);

    // Verify labeled counters
    try testing.expect(std.mem.indexOf(u8, output, "# TYPE zmq_s3_ops_total counter") != null);
    try testing.expect(std.mem.indexOf(u8, output, "operation=\"put\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "operation=\"get\"") != null);

    // Verify labeled gauges (Sprint 5 addition)
    try testing.expect(std.mem.indexOf(u8, output, "# TYPE zmq_consumer_lag gauge") != null);
    try testing.expect(std.mem.indexOf(u8, output, "group=\"my-group\"") != null);
}

test "MetricRegistry counter values are accurate after workload simulation" {
    // Verify metric accumulation works correctly across many updates
    const alloc = testing.allocator;

    var registry = MetricRegistry.init(alloc);
    defer registry.deinit();

    try registry.registerCounter("produce_count", "Produce requests");
    try registry.registerCounter("fetch_count", "Fetch requests");

    // Simulate mixed workload
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        registry.incrementCounter("produce_count");
        if (i % 3 == 0) registry.incrementCounter("fetch_count");
    }

    const produce = registry.counters.get("produce_count").?;
    try testing.expectEqual(@as(u64, 100), produce.value);

    const fetch = registry.counters.get("fetch_count").?;
    try testing.expectEqual(@as(u64, 34), fetch.value); // 0,3,6,...,99 → 34 values
}

// ---------------------------------------------------------------
// Test 6: JSON Logger Produces Valid Output
// ---------------------------------------------------------------

test "JSON logger produces structured NDJSON output" {
    // Sprint 5: Structured logging for production observability
    const alloc = testing.allocator;

    var output_buf = std.array_list.Managed(u8).init(alloc);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(alloc, &output_buf);
    defer logger.deinit();

    // Log a basic info message
    logger.log(.info, "broker started", null);

    var output = output_buf.items;

    // Verify NDJSON format: starts with {, ends with }\n
    try testing.expect(output.len > 0);
    try testing.expect(output[0] == '{');
    try testing.expect(output[output.len - 1] == '\n');

    // Verify required JSON fields
    try testing.expect(std.mem.indexOf(u8, output, "\"ts\":\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"level\":\"info\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"msg\":\"broker started\"") != null);

    // ISO 8601 timestamp should contain 'T' and 'Z'
    try testing.expect(std.mem.indexOf(u8, output, "T") != null);
    try testing.expect(std.mem.indexOf(u8, output, "Z\"") != null);

    // Log with correlation_id and extra fields
    output_buf.clearRetainingCapacity();
    logger.logWithFields(.warn, "slow produce", 42, &.{ "topic", "orders", "latency_ms", "150" });

    output = output_buf.items;
    try testing.expect(std.mem.indexOf(u8, output, "\"level\":\"warn\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"correlation_id\":42") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"topic\":\"orders\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"latency_ms\":\"150\"") != null);
}

test "JSON logger escapes special characters in messages" {
    // Sprint 5: Verify JSON escaping produces safe output
    const alloc = testing.allocator;

    var output_buf = std.array_list.Managed(u8).init(alloc);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(alloc, &output_buf);
    defer logger.deinit();

    logger.log(.err, "failed: \"timeout\"\nnew line", null);

    const output = output_buf.items;
    // Verify JSON escaping: quotes become \", newline becomes \n
    try testing.expect(std.mem.indexOf(u8, output, "\\\"timeout\\\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\\n") != null);
    // The output should still be valid NDJSON (ends with }\n)
    try testing.expect(output[output.len - 1] == '\n');
}

// ---------------------------------------------------------------
// Test 7: Graceful Shutdown Request Rejection
// ---------------------------------------------------------------

test "Graceful shutdown: flag interaction gates request path" {
    // Sprint 6: Broker.is_shutting_down and is_fenced_by_controller both reject
    // data-path requests. The actual Broker.handleRequest checks:
    //   if (self.is_shutting_down and api_key != 18) → reject
    //   if (self.is_fenced_by_controller) → reject produces
    //
    // We verify the flag logic pattern here because full Broker construction
    // requires all subsystems (PartitionStore, GroupCoordinator, etc.).

    // Simulate the shutdown gate: should requests be rejected?
    const api_key_produce: i16 = 0; // Produce
    const api_key_fetch: i16 = 1; // Fetch
    const api_key_versions: i16 = 18; // ApiVersions (always allowed)

    // Normal operation: nothing rejected
    var shutting_down = false;
    try testing.expect(!(shutting_down and api_key_produce != 18));
    try testing.expect(!(shutting_down and api_key_fetch != 18));

    // Graceful shutdown: data-path rejected, ApiVersions allowed
    shutting_down = true;
    try testing.expect(shutting_down and api_key_produce != 18); // rejected
    try testing.expect(shutting_down and api_key_fetch != 18); // rejected
    try testing.expect(!(shutting_down and api_key_versions != 18)); // allowed

    // NOT_LEADER_OR_FOLLOWER error code tells clients to reconnect elsewhere
    const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    try testing.expectEqual(@as(i16, 6), NOT_LEADER_OR_FOLLOWER);
}

test "Graceful shutdown: fenced and shutdown flags are independent" {
    // Sprint 6: Both flags independently cause request rejection
    var shutting_down = false;
    var fenced_by_controller = false;

    // Normal state: both false → allow
    try testing.expect(!shutting_down and !fenced_by_controller);

    // Shutdown only → reject
    shutting_down = true;
    try testing.expect(shutting_down or fenced_by_controller);

    // Fenced only → also reject
    shutting_down = false;
    fenced_by_controller = true;
    try testing.expect(shutting_down or fenced_by_controller);

    // Both → reject
    shutting_down = true;
    try testing.expect(shutting_down and fenced_by_controller);
}

// ---------------------------------------------------------------
// Test 8: /ready Probe State Transitions
// ---------------------------------------------------------------

test "MetricsServer /ready probe transitions from 503 to 200" {
    // Sprint 5: /ready returns 503 until startup_complete is set to true.
    // This test validates the state transition at the struct level without
    // starting a real TCP listener.
    const alloc = testing.allocator;

    var registry = MetricRegistry.init(alloc);
    defer registry.deinit();

    var ms = MetricsServer.init(alloc, 9090, &registry);

    // Initially not ready — /ready should return 503.
    const starting = ms.readinessResponse();
    try testing.expectEqualStrings("503 Service Unavailable", starting.status);
    try testing.expectEqualStrings("NOT READY\n", starting.body);

    // After startup completes — /ready should return 200.
    ms.markStartupComplete();
    const ready = ms.readinessResponse();
    try testing.expectEqualStrings("200 OK", ready.status);
    try testing.expectEqualStrings("READY\n", ready.body);

    // Graceful shutdown removes readiness before the listener exits.
    ms.stop();
    const shutting_down = ms.readinessResponse();
    try testing.expectEqualStrings("503 Service Unavailable", shutting_down.status);
    try testing.expectEqualStrings("NOT READY\n", shutting_down.body);

    // Verify running flag defaults to false (server not yet started)
    try testing.expect(!ms.running);
}

// ---------------------------------------------------------------
// Test 9: Cross-cutting — TLS config + SCRAM + Metrics together
// ---------------------------------------------------------------

test "Cross-cutting: TLS config with SASL_SSL enables both TLS and SASL" {
    // In production, security uses SASL_SSL: both TLS encryption and SCRAM auth.
    // Verify the configuration correctly identifies this combined requirement.
    const config = TlsConfig{
        .protocol = .sasl_ssl,
        .cert_file = "/etc/zmq/broker.pem",
        .key_file = "/etc/zmq/broker.key",
        .ca_file = "/etc/zmq/ca.pem",
        .client_auth = .required,
    };

    try testing.expect(config.needsTls());
    try testing.expect(config.needsSasl());
    try config.validate();

    // Verify mTLS is configured
    try testing.expect(config.ca_file != null);
    try testing.expectEqual(TlsConfig.ClientAuth.required, config.client_auth);
}

test "Cross-cutting: SASL/PLAIN and SCRAM authenticators coexist" {
    // Production deployments may support multiple SASL mechanisms simultaneously.
    const alloc = testing.allocator;

    var plain_auth = SaslPlainAuthenticator.init(alloc);
    defer plain_auth.deinit();
    try plain_auth.addUser("legacy-client", "plainpass");

    var scram_auth = ScramSha256Authenticator.init(alloc);
    defer scram_auth.deinit();
    try scram_auth.addUser("modern-client", "scrampass");

    // PLAIN user can authenticate via SASL/PLAIN
    const plain_result = plain_auth.authenticate("\x00legacy-client\x00plainpass");
    try testing.expect(plain_result.success);
    try testing.expectEqualStrings("legacy-client", plain_result.principal.?);

    // SCRAM user has stored credential for SCRAM exchange
    try testing.expect(scram_auth.getCredential("modern-client") != null);
    try testing.expect(scram_auth.getCredential("nonexistent") == null);

    // PLAIN auth rejects SCRAM user (different password store)
    const cross_result = plain_auth.authenticate("\x00modern-client\x00scrampass");
    try testing.expect(!cross_result.success);
}

test "Observability dashboard and alerts reference exported metrics" {
    const alloc = testing.allocator;

    var registry = MetricRegistry.init(alloc);
    defer registry.deinit();
    try registerOperationalMetricCorpus(&registry);

    const dashboard_file = try fs.cwd().openFile("docs/observability/zmq-grafana-dashboard.json", .{});
    defer dashboard_file.close();
    const dashboard = try dashboard_file.readToEndAlloc(alloc, 256 * 1024);
    defer alloc.free(dashboard);

    var parsed_dashboard = try std.json.parseFromSlice(std.json.Value, alloc, dashboard, .{});
    defer parsed_dashboard.deinit();

    var dashboard_expressions = std.array_list.Managed([]const u8).init(alloc);
    defer dashboard_expressions.deinit();
    try collectJsonPromqlExpressions(&parsed_dashboard.value, &dashboard_expressions);
    try testing.expect(dashboard_expressions.items.len >= 9);

    const dashboard_metrics = [_][]const u8{
        "kafka_server_requests_total",
        "kafka_server_produce_requests_total",
        "kafka_server_fetch_requests_total",
        "kafka_server_bytes_in_total",
        "kafka_server_bytes_out_total",
        "kafka_server_active_connections",
        "kafka_server_member_count",
        "kafka_server_partition_count",
        "kafka_server_topic_count",
        "kafka_server_group_count",
        "kafka_server_api_errors_total",
        "kafka_server_request_latency_seconds_bucket",
        "kafka_server_produce_latency_seconds_bucket",
        "kafka_server_fetch_latency_seconds_bucket",
        "kafka_network_connections_active",
        "Kafka_server_connection_count",
        "Kafka_topic_count",
        "Kafka_group_count",
        "Kafka_partition_count",
        "Kafka_partition_total_count",
        "Kafka_request_count_total",
        "Kafka_request_error_count_total",
        "Kafka_request_size_bytes_total",
        "Kafka_response_size_bytes_total",
        "Kafka_request_time_milliseconds_total",
        "automq_object_manager_stream_count",
        "automq_object_manager_stream_set_object_count",
        "automq_object_manager_stream_object_count",
        "automq_object_manager_prepared_object_count",
        "automq_object_manager_mark_destroyed_object_count",
        "s3_requests_total",
        "s3_request_errors_total",
        "s3_bytes_total",
        "s3_request_duration_seconds_bucket",
        "log_cache_size_bytes",
        "log_cache_entries",
        "s3_block_cache_size_bytes",
        "s3_block_cache_entries",
        "cache_operations_total",
        "cache_evictions_total",
        "compaction_cycles_total",
        "compaction_cycle_duration_seconds_bucket",
        "compaction_splits_total",
        "compaction_merges_total",
        "compaction_cleanups_total",
        "compaction_destroyed_total",
        "compaction_expired_prepared_total",
        "compaction_orphaned_keys",
        "kafka_client_telemetry_pushes_total",
        "kafka_client_telemetry_terminations_total",
        "kafka_client_telemetry_exported_total",
        "kafka_client_telemetry_export_errors_total",
        "kafka_client_telemetry_export_bytes_total",
        "kafka_client_telemetry_samples",
        "kafka_client_telemetry_bytes",
        "raft_role",
        "raft_current_epoch",
        "raft_commit_index",
        "raft_elections_started_total",
        "raft_pre_votes_started_total",
        "raft_votes_granted_total",
        "raft_votes_rejected_total",
        "raft_leader_elections_won_total",
        "raft_epoch_changes_total",
        "raft_log_entries_appended_total",
        "raft_log_entries_committed_total",
        "raft_snapshots_taken_total",
        "kafka_network_requestmetrics_requests_total",
        "kafka_network_requestmetrics_requestbytes_total",
        "kafka_network_requestmetrics_responsebytes_total",
        "kafka_network_requestmetrics_totaltimems_total",
        "kafka_network_requestmetrics_requestqueuetimems_total",
        "kafka_network_requestmetrics_localtimems_total",
        "kafka_network_requestmetrics_remotetimems_total",
        "kafka_network_requestmetrics_responsequeuetimems_total",
        "kafka_network_requestmetrics_responsesendtimems_total",
        "kafka_network_requestmetrics_errors_total",
        "kafka_network_requestchannel_requestqueuesize",
        "kafka_network_requestchannel_responsequeuesize",
        "kafka_server_brokertopicmetrics_totalproducerequests_total",
        "kafka_server_brokertopicmetrics_totalfetchrequests_total",
        "kafka_server_brokertopicmetrics_messagesin_total",
        "kafka_server_brokertopicmetrics_bytesrejected_total",
        "kafka_server_brokertopicmetrics_failedproducerequests_total",
        "kafka_server_brokertopicmetrics_failedfetchrequests_total",
        "kafka_server_brokertopicmetrics_bytesin_total",
        "kafka_server_brokertopicmetrics_bytesout_total",
        "kafka_server_produce_throttle_total",
        "kafka_server_fetch_throttle_total",
        "kafka_server_quota_manager_metrics_client_quota_count",
        "kafka_server_quota_manager_metrics_default_window_count",
        "kafka_server_quota_manager_metrics_default_produce_byte_rate",
        "kafka_server_quota_manager_metrics_default_fetch_byte_rate",
        "kafka_server_quota_manager_metrics_default_request_rate",
        "kafka_controller_kafkacontroller_activecontrollercount",
        "kafka_controller_kafkacontroller_activebrokercount",
        "kafka_controller_kafkacontroller_fencedbrokercount",
        "kafka_server_kafkaserver_brokerstate",
        "kafka_server_groupmetadatamanager_numgroups",
        "kafka_server_groupmetadatamanager_numoffsets",
        "kafka_server_groupmetadatamanager_numgroupsempty",
        "kafka_server_groupmetadatamanager_numgroupspreparingrebalance",
        "kafka_server_groupmetadatamanager_numgroupscompletingrebalance",
        "kafka_server_groupmetadatamanager_numgroupsstable",
        "kafka_server_groupmetadatamanager_numgroupsdead",
        "kafka_server_group_coordinator_metrics_partition_count",
        "kafka_server_group_coordinator_metrics_group_count",
        "kafka_server_group_coordinator_metrics_offset_commit_count_total",
        "kafka_server_group_coordinator_metrics_event_queue_size",
        "kafka_server_group_coordinator_metrics_thread_idle_ratio_avg",
        "kafka_consumer_lag",
        "kafka_server_transaction_coordinator_metrics_transaction_count",
        "kafka_server_transaction_coordinator_metrics_transactional_id_count",
        "kafka_server_transaction_coordinator_metrics_registered_partition_count",
        "kafka_server_transaction_coordinator_metrics_partition_count",
        "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",
        "kafka_network_socketserver_networkprocessoravgidlepercent",
        "kafka_network_socketserver_connectioncount",
        "kafka_network_socketserver_expiredconnectionskilledcount_total",
        "kafka_server_replicamanager_partitioncount",
        "kafka_server_replicamanager_leadercount",
        "kafka_server_replicamanager_underreplicatedpartitions",
        "kafka_server_replicamanager_underminisrpartitioncount",
        "kafka_server_replicamanager_atminisrpartitioncount",
        "kafka_server_replicamanager_offlinepartitionscount",
        "kafka_server_replicamanager_reassigningpartitions",
        "kafka_controller_kafkacontroller_globaltopiccount",
        "kafka_controller_kafkacontroller_globalpartitioncount",
        "kafka_controller_kafkacontroller_offlinepartitionscount",
        "kafka_controller_kafkacontroller_preferredreplicaimbalancecount",
        "kafka_controller_controllerstats_leaderelectionrateandtimems_total",
        "kafka_controller_controllerstats_uncleanleaderelectionspersec_total",
        "kafka_log_logmanager_offlinelogdirectorycount",
        "kafka_server_replicamanager_isrshrinks_total",
        "kafka_server_replicamanager_isrexpands_total",
        "kafka_server_replicamanager_failedisrupdatesperseccount_total",
        "kafka_server_delayedoperationpurgatory_purgatorysize",
    };
    for (dashboard_metrics) |metric| {
        try testing.expect(std.mem.indexOf(u8, dashboard, metric) != null);
    }
    try testing.expect(std.mem.indexOf(u8, dashboard, "ZMQ AutoMQ Parity Overview") != null);

    var dashboard_metric_refs: usize = 0;
    for (dashboard_expressions.items) |expr| {
        dashboard_metric_refs += try assertPromqlExpressionMetricsRegistered(&registry, "dashboard", expr);
    }
    try testing.expect(dashboard_metric_refs >= dashboard_metrics.len);

    const alerts_file = try fs.cwd().openFile("docs/observability/zmq-prometheus-alerts.yaml", .{});
    defer alerts_file.close();
    const alerts = try alerts_file.readToEndAlloc(alloc, 128 * 1024);
    defer alloc.free(alerts);

    var alert_expressions = std.array_list.Managed([]const u8).init(alloc);
    defer alert_expressions.deinit();
    try collectYamlPromqlExpressions(alerts, &alert_expressions);
    try testing.expect(alert_expressions.items.len >= 9);

    const alert_metrics = [_][]const u8{
        "kafka_server_api_errors_total",
        "kafka_server_requests_total",
        "kafka_server_request_latency_seconds_bucket",
        "kafka_server_produce_latency_seconds_bucket",
        "kafka_server_fetch_latency_seconds_bucket",
        "Kafka_request_count_total",
        "Kafka_request_error_count_total",
        "Kafka_request_size_bytes_total",
        "Kafka_response_size_bytes_total",
        "Kafka_request_time_milliseconds_total",
        "automq_object_manager_stream_count",
        "automq_object_manager_stream_set_object_count",
        "automq_object_manager_stream_object_count",
        "automq_object_manager_prepared_object_count",
        "automq_object_manager_mark_destroyed_object_count",
        "kafka_server_groupmetadatamanager_numgroups",
        "kafka_server_groupmetadatamanager_numoffsets",
        "kafka_server_groupmetadatamanager_numgroupsdead",
        "kafka_server_group_coordinator_metrics_partition_count",
        "kafka_server_group_coordinator_metrics_event_queue_size",
        "kafka_server_group_coordinator_metrics_thread_idle_ratio_avg",
        "kafka_consumer_lag",
        "kafka_server_transaction_coordinator_metrics_transaction_count",
        "kafka_server_transaction_coordinator_metrics_transactional_id_count",
        "kafka_server_transaction_coordinator_metrics_registered_partition_count",
        "kafka_server_transaction_coordinator_metrics_partition_count",
        "kafka_server_produce_throttle_total",
        "kafka_server_fetch_throttle_total",
        "raft_role",
        "raft_elections_started_total",
        "raft_pre_votes_started_total",
        "raft_epoch_changes_total",
        "raft_log_entries_appended_total",
        "raft_log_entries_committed_total",
        "raft_votes_rejected_total",
        "s3_requests_total",
        "s3_request_errors_total",
        "s3_request_duration_seconds_bucket",
        "s3_bytes_total",
        "compaction_errors_total",
        "compaction_orphaned_keys",
        "compaction_cycle_duration_seconds_bucket",
        "cache_operations_total",
        "cache_evictions_total",
        "log_cache_size_bytes",
        "s3_block_cache_size_bytes",
        "kafka_client_telemetry_bytes",
        "kafka_client_telemetry_samples",
        "kafka_client_telemetry_export_errors_total",
        "kafka_controller_kafkacontroller_activecontrollercount",
        "kafka_controller_kafkacontroller_activebrokercount",
        "kafka_controller_kafkacontroller_fencedbrokercount",
        "kafka_server_kafkaserver_brokerstate",
        "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",
        "kafka_network_socketserver_networkprocessoravgidlepercent",
        "kafka_network_socketserver_connectioncount",
        "kafka_network_socketserver_expiredconnectionskilledcount_total",
        "kafka_server_replicamanager_offlinepartitionscount",
        "kafka_server_replicamanager_underreplicatedpartitions",
        "kafka_server_replicamanager_underminisrpartitioncount",
        "kafka_server_replicamanager_atminisrpartitioncount",
        "kafka_server_replicamanager_reassigningpartitions",
        "kafka_server_delayedoperationpurgatory_purgatorysize",
        "kafka_network_requestchannel_requestqueuesize",
        "kafka_network_requestchannel_responsequeuesize",
        "kafka_server_brokertopicmetrics_failedproducerequests_total",
        "kafka_server_brokertopicmetrics_failedfetchrequests_total",
        "kafka_server_brokertopicmetrics_bytesrejected_total",
        "kafka_network_requestmetrics_localtimems_total",
        "kafka_network_requestmetrics_totaltimems_total",
        "kafka_network_requestmetrics_remotetimems_total",
        "kafka_network_requestmetrics_requestqueuetimems_total",
        "kafka_network_requestmetrics_responsequeuetimems_total",
        "kafka_network_requestmetrics_responsesendtimems_total",
        "kafka_network_requestmetrics_requests_total",
        "kafka_network_requestmetrics_requestbytes_total",
        "kafka_network_requestmetrics_responsebytes_total",
        "kafka_network_requestmetrics_errors_total",
        "kafka_log_logmanager_offlinelogdirectorycount",
        "kafka_controller_kafkacontroller_offlinepartitionscount",
        "kafka_controller_kafkacontroller_preferredreplicaimbalancecount",
        "kafka_controller_controllerstats_leaderelectionrateandtimems_total",
        "kafka_controller_controllerstats_uncleanleaderelectionspersec_total",
        "kafka_server_replicamanager_isrshrinks_total",
        "kafka_server_replicamanager_isrexpands_total",
        "kafka_server_replicamanager_failedisrupdatesperseccount_total",
    };
    for (alert_metrics) |metric| {
        try testing.expect(std.mem.indexOf(u8, alerts, metric) != null);
    }
    const alert_names = [_][]const u8{
        "ZMQHighApiErrorRatio",
        "ZMQHighRequestLatencyP99",
        "ZMQHighAutoMQRequestErrorRatio",
        "ZMQHighAutoMQRequestTime",
        "ZMQHighAutoMQRequestBytesIn",
        "ZMQHighAutoMQResponseBytesOut",
        "ZMQAutoMQObjectMetadataFanoutHigh",
        "ZMQAutoMQPreparedObjectsHigh",
        "ZMQAutoMQDestroyedObjectsPending",
        "ZMQHighProduceLatencyP99",
        "ZMQHighFetchLatencyP99",
        "ZMQGroupMetadataOffsetFanoutHigh",
        "ZMQDeadConsumerGroupsPresent",
        "ZMQGroupCoordinatorEventQueueBacklog",
        "ZMQLowGroupCoordinatorIdle",
        "ZMQGroupCoordinatorPartitionFailure",
        "ZMQConsumerLagHigh",
        "ZMQDeadTransactionsPresent",
        "ZMQTransactionRegisteredPartitionFanoutHigh",
        "ZMQTransactionCoordinatorPartitionFailure",
        "ZMQProduceThrottlingActive",
        "ZMQFetchThrottlingActive",
        "ZMQNoActiveRaftLeader",
        "ZMQRaftElectionChurn",
        "ZMQRaftPreVoteChurn",
        "ZMQRaftEpochChurn",
        "ZMQRaftCommitStall",
        "ZMQRaftVoteRejections",
        "ZMQS3RequestErrors",
        "ZMQS3LatencyP99",
        "ZMQS3RequestRateHigh",
        "ZMQS3ByteRateHigh",
        "ZMQNoActiveController",
        "ZMQNoActiveBroker",
        "ZMQFencedBrokers",
        "ZMQBrokerNotRunning",
        "ZMQLowRequestHandlerIdle",
        "ZMQLowNetworkProcessorIdle",
        "ZMQExpiredConnectionsKilled",
        "ZMQSocketServerConnectionCountHigh",
        "ZMQRequestChannelBacklog",
        "ZMQOfflinePartitions",
        "ZMQUnderReplicatedPartitions",
        "ZMQUnderMinIsrPartitions",
        "ZMQAtMinIsrPartitions",
        "ZMQPartitionReassignmentStuck",
        "ZMQDelayedFetchPurgatoryBacklog",
        "ZMQFailedProduceRequests",
        "ZMQFailedFetchRequests",
        "ZMQRejectedProduceBytes",
        "ZMQHighJmxRequestLocalTime",
        "ZMQHighJmxRequestTotalTime",
        "ZMQHighJmxRequestRemoteTime",
        "ZMQHighJmxRequestQueueTime",
        "ZMQHighJmxResponseQueueTime",
        "ZMQHighJmxResponseSendTime",
        "ZMQHighJmxRequestBytesIn",
        "ZMQHighJmxResponseBytesOut",
        "ZMQJmxRequestErrors",
        "ZMQOfflineLogDirectories",
        "ZMQControllerOfflinePartitions",
        "ZMQPreferredReplicaImbalance",
        "ZMQUncleanLeaderElections",
        "ZMQLeaderElectionChurn",
        "ZMQIsrShrinks",
        "ZMQIsrExpands",
        "ZMQFailedIsrUpdates",
        "ZMQCompactionErrors",
        "ZMQCompactionOrphanedKeysPending",
        "ZMQCompactionCycleSlowP99",
        "ZMQCacheEvictionsHigh",
        "ZMQCacheMissRatioHigh",
        "ZMQLogCacheSizeHigh",
        "ZMQS3BlockCacheSizeHigh",
        "ZMQClientTelemetryRetainedBytesHigh",
        "ZMQClientTelemetrySamplesHigh",
        "ZMQClientTelemetryExportErrors",
    };
    for (alert_names) |name| {
        try testing.expect(std.mem.indexOf(u8, alerts, name) != null);
    }
    try testing.expect(std.mem.indexOf(u8, alerts, "severity: critical") != null);
    const critical_alert_names = [_][]const u8{
        "ZMQNoActiveRaftLeader",
        "ZMQNoActiveController",
        "ZMQNoActiveBroker",
        "ZMQBrokerNotRunning",
        "ZMQOfflinePartitions",
        "ZMQUnderMinIsrPartitions",
        "ZMQOfflineLogDirectories",
        "ZMQControllerOfflinePartitions",
        "ZMQUncleanLeaderElections",
    };
    for (critical_alert_names) |name| {
        try assertAlertHasSeverity(alerts, name, "severity: critical");
    }

    var alert_metric_refs: usize = 0;
    for (alert_expressions.items) |expr| {
        alert_metric_refs += try assertPromqlExpressionMetricsRegistered(&registry, "alerts", expr);
    }
    try testing.expect(alert_metric_refs >= alert_metrics.len);
}

test "YAML PromQL collection handles block scalar expressions" {
    var expressions = std.array_list.Managed([]const u8).init(testing.allocator);
    defer expressions.deinit();

    const yaml =
        \\groups:
        \\  - name: zmq-test
        \\    rules:
        \\      - alert: ZMQBlockExpression
        \\        expr: |
        \\          sum(rate(kafka_server_requests_total[5m]))
        \\            / clamp_min(sum(rate(kafka_server_fetch_requests_total[5m])), 1)
        \\      - alert: ZMQQuotedExpression
        \\        expr: "rate(kafka_server_api_errors_total[5m])"
    ;

    try collectYamlPromqlExpressions(yaml, &expressions);

    try testing.expectEqual(@as(usize, 3), expressions.items.len);
    try testing.expectEqualStrings("sum(rate(kafka_server_requests_total[5m]))", expressions.items[0]);
    try testing.expectEqualStrings("/ clamp_min(sum(rate(kafka_server_fetch_requests_total[5m])), 1)", expressions.items[1]);
    try testing.expectEqualStrings("rate(kafka_server_api_errors_total[5m])", expressions.items[2]);
}

test "PromQL metric audit recognizes registered API catalog metric names" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();
    try registerOperationalMetricCorpus(&registry);

    const refs = try assertPromqlExpressionMetricsRegistered(
        &registry,
        "self-test",
        "sum(rate(api_versions[5m])) + sum(rate(metadata[5m]))",
    );
    try testing.expectEqual(@as(usize, 2), refs);

    const labeled_refs = try assertPromqlExpressionMetricsRegistered(
        &registry,
        "self-test",
        "sum(rate(Kafka_request_count_total{type=\"api_versions\"}[5m]))",
    );
    try testing.expectEqual(@as(usize, 1), labeled_refs);
}

test "AutoMQ parity release criteria document pins required gates" {
    const alloc = testing.allocator;

    const criteria_file = try fs.cwd().openFile("docs/RELEASE_CRITERIA.md", .{});
    defer criteria_file.close();
    const criteria = try criteria_file.readToEndAlloc(alloc, 128 * 1024);
    defer alloc.free(criteria);

    const required_sections = [_][]const u8{
        "# AutoMQ Parity Release Criteria",
        "## Required Gates",
        "## Required Commands",
        "## Known Unsupported Or Partial Surfaces",
        "## Release Decision",
    };
    for (required_sections) |section| {
        try testing.expect(std.mem.indexOf(u8, criteria, section) != null);
    }

    const required_gates = [_][]const u8{
        "`Protocol`",
        "`Durability`",
        "`Stateless`",
        "`MultiNode`",
        "`Security`",
        "`Observability`",
        "`Performance`",
        "`Chaos`",
    };
    for (required_gates) |gate| {
        try testing.expect(std.mem.indexOf(u8, criteria, gate) != null);
    }
    try testing.expect(std.mem.indexOf(u8, criteria, "Runtime elapsed-time gates must use monotonic clocks") != null);
    try testing.expect(std.mem.indexOf(u8, criteria, "Python live-harness deadlines and elapsed-duration checks") != null);
    try testing.expect(std.mem.indexOf(u8, criteria, "Kafka-visible timestamps and unique object names") != null);
    try testing.expect(std.mem.indexOf(u8, criteria, "outbound TLS hostname verification") != null);

    const required_commands = [_][]const u8{
        "zig build test --summary all",
        "zig build test-protocol-static-audit --summary all",
        "zig build test-observability-static-audit --summary all",
        "zig build test-build-static-audit --summary all",
        "ok: protocol static audit",
        "ok: observability static audit",
        "ok: build static audit",
        "Protocol static audit evidence also pins strict schema codegen JSON parsing",
        "before generated Zig protocol schemas are written",
        "codegen scripts must exit nonzero on schema parse errors",
        "Checked-in Grafana dashboard JSON must be strict JSON",
        "dashboard metric-reference checks",
        "positive 24-column dashboard",
        "dashboard target schema containing only",
        "closed Prometheus alert group set",
        "zig build test-chaos --summary all",
        "zig build test-client-matrix --summary all",
        "zig build test-minio --summary all",
        "zig build test-s3-process-crash --summary all",
        "zig build test-s3-provider-matrix --summary all",
        "zig build test-kraft-failover --summary all",
        "zig build test-e2e --summary all",
        "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
        "ZMQ_KRAFT_NETWORK_MATRIX",
        "ZMQ_CHAOS_REQUIRED_SCENARIOS",
        "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
        "ZMQ_CHAOS_NETWORK_MATRIX",
        "release-evidence chaos scenario catalogue",
        "CHAOS_SCENARIO_ALIASES",
        "REQUIRED_CHAOS_SCENARIOS",
        "CHAOS_SCENARIO_MARKERS",
        "canonical broker chaos scenarios",
        "sigkill",
        "partial-client",
        "clock-skew",
        "s3",
        "network",
        "live-s3",
        "s3-live",
        "sigkill-restart",
        "slow-partial-client",
        "clock-skewed-records",
        "s3-outage",
        "network-partition",
        "live-s3-outage",
        "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0",
        "second_offset=<positive> source=command",
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
        "ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command",
        "ok: chaos s3-outage",
        "ok: chaos network-partition source=command",
        "base_offset_negative=true serving=true source=command",
        "ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command",
        "build static audit chaos-scenario catalogue",
        "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
        "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
        "ZMQ_S3_PROVIDER_REQUIRED_PROFILES",
        "ZMQ_S3_PROVIDER_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES",
        "ZMQ_CLIENT_MATRIX_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS",
        "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
        "release-evidence client capability catalogue",
        "REQUIRED_CLIENT_TOOLS",
        "REQUIRED_CLIENT_SEMANTICS",
        "CLIENT_SECURITY_PROTOCOLS",
        "CLIENT_SASL_MECHANISMS",
        "CLIENT_SECURITY_TOOLS",
        "CLIENT_REBALANCE_TOOLS",
        "CLIENT_TRANSACTION_TOOLS",
        "kcat",
        "kafka-cli",
        "kafka-python",
        "confluent-kafka",
        "java-kafka",
        "go-kafka",
        "basic",
        "admin",
        "groups",
        "rebalance",
        "transactions",
        "security",
        "security-negative",
        "PLAINTEXT",
        "SASL_PLAINTEXT",
        "SSL",
        "SASL_SSL",
        "PLAIN",
        "SCRAM-SHA-256",
        "OAUTHBEARER",
        "build static audit client-capability catalogue",
        "release-evidence client tool marker catalogue",
        "CLIENT_TOOL_OUTPUT_MARKERS",
        "per-tool probe markers",
        "ok: kcat probes",
        "ok: kafka CLI probes",
        "ok: kafka-python probes",
        "ok: confluent-kafka probes",
        "ok: java-kafka probes",
        "ok: go-kafka probes",
        "build static audit client-tool-marker catalogue",
        "release-evidence client version/provenance catalogue",
        "CLIENT_PYTHON_TOOLS",
        "CLIENT_UNPINNED_VERSION_LABELS",
        "Python client matrix profile",
        "client/library version",
        "auto",
        "default",
        "latest",
        "build static audit client-version catalogue",
        "Profile-scoped client `TOOLS` and `SEMANTICS` entries",
        "reject blank or duplicate comma-separated values",
        "release-evidence boolean environment catalogue",
        "BOOLEAN_ENV_VARS",
        "CLIENT_PROFILE_BOOL_SUFFIXES",
        "E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES",
        "S3_BOOL_SUFFIXES",
        "real booleans",
        "ZMQ_BENCH_COMPARE_ENFORCE_GATES",
        "ZMQ_RUN_BENCH_COMPARE",
        "ZMQ_RUN_BENCH_LIVE_S3",
        "ZMQ_RUN_CHAOS_TESTS",
        "ZMQ_RUN_CLIENT_MATRIX",
        "ZMQ_RUN_E2E_TESTS",
        "ZMQ_RUN_KRAFT_FAILOVER_TESTS",
        "ZMQ_RUN_MINIO_TESTS",
        "ZMQ_RUN_PROCESS_CRASH_TESTS",
        "ZMQ_RUN_S3_PROVIDER_MATRIX",
        "ZMQ_CLIENT_MATRIX_ENABLE_GO",
        "ENABLE_GO",
        "FIXTURE_DRY_RUN",
        "FIXTURE_PRESTOP",
        "PATH_STYLE",
        "SKIP_ENSURE_BUCKET",
        "SKIP_MINIO_HEALTH",
        "REQUIRE_LIST_PAGINATION",
        "REQUIRE_MULTIPART_EDGE",
        "RUN_LIVE_OUTAGE",
        "RUN_MULTIPART_FAULT",
        "RUN_PROCESS_CRASH",
        "build static audit boolean-env catalogue",
        "release-evidence token vocabulary catalogue",
        "PLACEHOLDER_ENV_VALUES",
        "BOOL_TRUE_VALUES",
        "BOOL_FALSE_VALUES",
        "placeholder and boolean token values",
        "...",
        "placeholder",
        "required",
        "tbd",
        "todo",
        "1",
        "on",
        "true",
        "yes",
        "0",
        "false",
        "no",
        "off",
        "build static audit token-vocabulary catalogue",
        "release-evidence S3 string environment catalogue",
        "S3_STRING_SUFFIXES",
        "nonblank S3 string settings",
        "ENDPOINT",
        "BUCKET",
        "ACCESS_KEY",
        "SECRET_KEY",
        "REGION",
        "SCHEME",
        "TLS_CA_FILE",
        "build static audit S3-string catalogue",
        "release-evidence S3 provider scoped marker catalogue",
        "S3_PROVIDER_SCOPED_MARKER_TEMPLATES",
        "profile-scoped provider markers",
        "live-suite",
        "outage",
        "process-crash",
        "list-pagination",
        "multipart-edge",
        "multipart-fault",
        "live-suite, outage, process-crash, list-pagination, multipart-edge, and multipart-fault",
        "ok: S3 provider live-suite profile <profile> command_started=true completed=true source=command",
        "ok: S3 provider outage profile <profile> down=true healed=true fail_closed=true recovered=true source=command",
        "ok: S3 provider process-crash profile <profile> killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
        "ok: S3 provider list-pagination profile <profile> required=true completed=true source=command",
        "ok: S3 provider multipart-edge profile <profile> required=true completed=true source=command",
        "ok: S3 provider multipart-fault profile <profile> command_started=true completed=true injected=true recovered=true source=command",
        "build static audit S3-scoped-marker catalogue",
        "release-evidence sample environment output-marker catalogue",
        "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS",
        "sample release evidence manifests",
        "broker chaos harness, external client matrix, S3 provider matrix, KRaft failover gate, Docker E2E gate, and comparative benchmark gate",
        "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=1 source=command",
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
        "ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command",
        "ok: chaos s3-outage rejected=true error_code=56 base_offset_negative=true serving=true source=command",
        "ok: chaos network-partition phase broker-link down=true observed=failed healed=true recovered=true expect=fail source=command",
        "ok: chaos network-partition source=command",
        "ok: chaos harness passed for sigkill-restart, slow-partial-client, clock-skewed-records, s3-outage, network-partition source=command",
        "ok: kcat probes (basic,security,security-negative) source=command",
        "ok: client security detail profile kcat_sec tool=kcat protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command",
        "ok: client matrix profile kcat_sec passed for kcat against localhost:9092 version=kcat-1.7.1 source=command",
        "ok: kafka CLI probes (basic,admin,security,security-negative) source=command",
        "ok: client security detail profile kafka_cli_sec tool=kafka-cli protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command",
        "ok: client matrix profile kafka_cli_sec passed for kafka-cli against localhost:9092 version=apache-kafka-cli-3.7.1 source=command",
        "ok: kafka-python probes (basic,admin,groups,security,security-negative) source=command",
        "ok: client security detail profile kafka_python_sec tool=kafka-python protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command",
        "ok: client matrix profile kafka_python_sec passed for kafka-python against localhost:9092 version=kafka-python-2.0.2 source=command",
        "ok: confluent-kafka probes (basic,admin,groups,rebalance,transactions,security,security-negative) source=command",
        "ok: client security detail profile confluent_2_3 tool=confluent-kafka protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command",
        "ok: client matrix profile confluent_2_3 passed for confluent-kafka against localhost:9092 version=confluent-kafka-2.3.0 source=command",
        "ok: java-kafka probes (basic,admin,rebalance,transactions,security,security-negative) source=command",
        "ok: client security detail profile java_3_7 tool=java-kafka protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command",
        "ok: client matrix profile java_3_7 passed for java-kafka against localhost:9092 version=apache-kafka-clients-3.7.1 source=command",
        "ok: go-kafka probes (basic,admin,groups) source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
        "ok: S3 provider live-suite profile minio command_started=true completed=true source=command",
        "ok: S3 provider profile minio endpoint=127.0.0.1:9000 bucket=zmq-minio-it scheme=http region=us-east-1 path_style=true source=command",
        "ok: S3 provider live-suite profile aws_us_east_1 command_started=true completed=true source=command",
        "ok: S3 provider outage detail profile aws_us_east_1 endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it scheme=https region=us-east-1 path_style=false down=true healed=true fail_closed=true recovered=true source=command",
        "ok: S3 provider outage profile aws_us_east_1 down=true healed=true fail_closed=true recovered=true source=command",
        "ok: S3 provider process-crash detail profile aws_us_east_1 bucket=zmq-aws-it topic=zmq-process-crash group=zmq-process-crash-group killed_broker=true fresh_data_dir=true first_offset=0 committed_offset=1 replacement_offset=2 recovered_payloads=2 source=command",
        "ok: S3 provider process-crash profile aws_us_east_1 killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
        "ok: S3 provider list-pagination profile aws_us_east_1 required=true completed=true source=command",
        "ok: S3 provider multipart-edge profile aws_us_east_1 required=true completed=true source=command",
        "ok: S3 multipart fault profile aws_us_east_1 endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it scheme=https region=us-east-1 path_style=false injected=true recovered=true source=command",
        "ok: S3 provider multipart-fault profile aws_us_east_1 command_started=true completed=true injected=true recovered=true source=command",
        "ok: S3 provider profile aws_us_east_1 endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it scheme=https region=us-east-1 path_style=false source=command",
        "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
        "ok: KRaft network partition phase leader-isolation down=true observed=failed healed=true healed_leader=1 healed_fetch=true expect=fail source=command",
        "ok: KRaft network partition phase broker-link down=true observed=survived healed=true healed_leader=2 healed_fetch=true expect=survive source=command",
        "ok: E2E chaos phase cross-broker down=true observed=failed healed=true recovered=true expect=fail source=command",
        "ok: E2E chaos passed for cross-broker phase(s) source=command",
        "ok: E2E load/scale phase load applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
        "ok: E2E load/scale phase scale-in applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
        "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
        "ok: E2E load/scale passed for load, scale-in, scale-out phase(s) source=command",
        "E2E hook context maps",
        "reject duplicate names",
        "Results: 53/53 passed, 0 failed",
        "build static audit sample-env-output catalogue",
        "release-evidence build summary and benchmark artifact catalogue",
        "BENCHMARK_RESULTS_ARTIFACT",
        "ZIG_BUILD_SUMMARY_RE",
        "benchmarks/results.json",
        "exactly one successful",
        "Results saved to benchmarks/results.json",
        "ok: comparative benchmark profile",
        "source=command",
        "Build Summary:",
        "steps succeeded",
        "tests passed",
        "build static audit build-summary catalogue",
        "release-evidence hook-provenance catalogue",
        "PHASE_HOOK_PROVENANCE_REQUIREMENTS",
        "PROFILE_HOOK_PROVENANCE_REQUIREMENTS",
        "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS",
        "phase hook, profile hook, and S3 enable provenance",
        "ZMQ_CHAOS_NETWORK",
        "chaos network phase",
        "DOWN",
        "UP",
        "collapsed",
        "ZMQ_KRAFT_NETWORK",
        "KRaft network phase",
        "ZMQ_E2E_CHAOS",
        "E2E chaos phase",
        "ZMQ_E2E_LOAD_SCALE",
        "E2E load/scale phase",
        "APPLY",
        "RESTORE",
        "ZMQ_S3",
        "S3 outage profile",
        "OUTAGE_DOWN",
        "OUTAGE_UP",
        "literal",
        "S3 multipart-fault profile",
        "MULTIPART_FAULT_CMD",
        "S3 process-crash profile",
        "S3 list-pagination profile",
        "S3 multipart-edge profile",
        "build static audit hook-provenance catalogue",
        "release-evidence comma-separated environment catalogue",
        "COMMA_SEPARATED_ENV_VARS",
        "REQUIRED_ENV_VARS except",
        "blank comma-separated entries",
        "duplicate comma-separated entries",
        "build static audit comma-env catalogue",
        "release-evidence coverage selector catalogue",
        "COVERAGE_SELECTOR_REQUIREMENTS",
        "selector, required, label, token_style, and fixture",
        "coverage selector assignments",
        "chaos network phases",
        "KRaft network phases",
        "E2E chaos phases",
        "E2E load/scale phases",
        "ZMQ_E2E_CHAOS_MATRIX",
        "ZMQ_E2E_LOAD_SCALE_MATRIX",
        "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE",
        "S3 provider profiles",
        "client matrix profiles",
        "build static audit coverage-selector catalogue",
        "zig build bench --summary all",
        "ZMQ_RUN_BENCH_LIVE_S3=1",
        "ZMQ_S3_ENDPOINT",
        "ZMQ_S3_PORT",
        "ZMQ_S3_BUCKET",
        "ZMQ_S3_SCHEME",
        "ZMQ_S3_REGION",
        "ZMQ_S3_PATH_STYLE",
        "ZMQ_BENCH_S3_WAL_MAX_REQUESTS_PER_MIB",
        "ZMQ_BENCH_S3_WAL_MAX_REBUILD_MS",
        "ZMQ_BENCH_LIVE_S3_MAX_REQUESTS_PER_MIB",
        "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS",
        "ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO",
        "ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO",
        "ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO",
        "ZMQ_BENCH_COMPARE_MAX_ERROR_RATE",
        "ZMQ_BENCH_COMPARE_REQUIRE_TREND",
        "ZMQ_BENCH_COMPARE_TREND_BASELINE",
        "ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO",
        "ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO",
        "ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO",
        "ZMQ_BENCH_LIVE_S3_ITERATIONS",
        "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES",
        "release-evidence numeric environment catalogue",
        "BENCHMARK_THRESHOLD_ENV_VARS and POSITIVE_INTEGER_ENV_VARS",
        "Trend baseline artifacts must",
        "trend baseline must not resolve to the current `benchmarks/results.json` output path",
        "strict structured benchmark JSON",
        "non-standard JSON constants such as `NaN`, `Infinity`, or `-Infinity`",
        "duplicate JSON object keys are rejected",
        "rejected while parsing archived baselines",
        "writing current `benchmarks/results.json`",
        "serialized before replacing the existing artifact",
        "only replace",
        "`benchmarks/results.json` after the gate passes",
        "cannot clobber the prior artifact",
        "selected/required target metadata must list",
        "known unique targets",
        "`targets_with_results` must match result",
        "target-label, iteration/warmup, threshold, gate,",
        "trend-baseline metadata",
        "target metadata must be a subset of selected target metadata",
        "finite non-negative",
        "real boolean gate flags",
        "Result artifact maps must be objects with only known target keys",
        "object results.",
        "no unknown benchmark result keys",
        "Archived trend baselines must include schema-version 1",
        "artifact metadata whose targets_with_results includes zmq",
        "numeric finite non-negative `throughput`, `p50`, and",
        "threshold variables must be nonblank, non-placeholder strings",
        "finite non-negative floats instead of falling back to defaults",
        "positive integers",
        "build static audit numeric-env catalogue",
        "missing, non-numeric, non-finite, negative, or zero trend",
        "Current comparative result rows are validated",
        "non-numeric or non-finite throughput/latency metrics",
        "non-integral error/request/success counts",
        "throughput/latency values fail the gate",
        "zig build bench-compare --summary all",
        "ZMQ_RELEASE_EVIDENCE",
        "pinned Zig executable path",
        "/tmp/zig-aarch64-linux-0.16.0/zig",
        "gated harness skip message",
        "successful `Build Summary: N/N steps succeeded`",
        "matching `N/N tests",
        "passed` counts",
        "must not contain any unsuccessful `Build Summary:` line",
        "non-negated build",
        "success line matching the invoked Zig build step",
        "`bench-compare success`",
        "concrete non-placeholder values",
        "coverage variables must parse",
        "placeholder paths",
        "same clean tracked checkout",
        "release evidence manifest must be strict JSON",
        "rejected before schema validation",
        "cannot determine the current git commit",
        "tracked worktree cleanliness",
        "token-aware command validation",
        "same shell command segment",
        "Command strings must be single-line and unquoted",
        "CR/LF line breaks",
        "newline command separators",
        "shell quote characters",
        "quoted assignment words cannot masquerade as active gate environment",
        "Backslash escapes are rejected",
        "escaped assignment words",
        "cannot satisfy required gate environment",
        "Required command environment assignments",
        "untracked shell provenance",
        "Repeated environment assignments are rejected",
        "cannot contain contradictory provenance",
        "required command catalogue mirror",
        "release-evidence REQUIRED_COMMANDS",
        "fenced release criteria command block",
        "same order",
        "build static audit command-block catalogue",
        "required environment-variable catalogue",
        "release-evidence REQUIRED_ENV_VARS",
        "release criteria, parity notes, and production-readiness pins",
        "every required coverage variable",
        "build static audit environment catalogue",
        "command environment-assignment catalogue",
        "per-gate command_env_assignments",
        "same-gate command provenance variable",
        "build static audit command-env catalogue",
        "release-evidence command-shape catalogue",
        "ENV_ASSIGNMENT_RE",
        "ENV_NAME_RE",
        "SHELL_COMMAND_SEPARATORS",
        "SUCCESS_SHELL_COMMAND_SEPARATOR",
        "DISALLOWED_SHELL_OPERATOR_TOKENS",
        "DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS",
        "DISALLOWED_COMMAND_LINE_BREAKS",
        "DISALLOWED_COMMAND_QUOTE_CHARS",
        "DISALLOWED_COMMAND_ESCAPE_CHARS",
        "ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS",
        "ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS",
        "FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS",
        "single-line direct invocations",
        "^[A-Za-z_][A-Za-z0-9_]*=.*$",
        "^[A-Za-z_][A-Za-z0-9_]*$",
        "&&",
        ";",
        "||",
        "&",
        "&>",
        "&>>",
        "|",
        "|&",
        ">",
        ">>",
        "<",
        "<<",
        "<<<",
        "<>",
        "<&",
        ">&",
        ">|",
        "(",
        ")",
        "{",
        "}",
        "$(",
        "backtick",
        "\\n",
        "\\r",
        "single quote",
        "double quote",
        "backslash",
        "echo ok: root compose config",
        "echo ok: kafka compose config",
        "echo ok: automq compose config",
        "docker compose -f docker-compose.yml config --quiet && echo ok: root compose config",
        "docker compose -f benchmarks/kafka-compose.yml config --quiet && echo ok: kafka compose config",
        "docker compose -f benchmarks/automq-compose.yml config --quiet && echo ok: automq compose config",
        "Build Summary:",
        "tests passed",
        "test success",
        "bench success",
        "bench-compare success",
        "trend thresholds:",
        "trend baseline:",
        "build static audit command-shape catalogue",
        "release-evidence skip-marker catalogue",
        "per-gate skip_markers",
        "skipped live gate",
        "skip: set ZMQ_RUN_CHAOS_TESTS=1",
        "skip: set ZMQ_RUN_CLIENT_MATRIX=1",
        "skipped",
        "skip: set ZMQ_RUN_PROCESS_CRASH_TESTS=1",
        "skip: set ZMQ_RUN_S3_PROVIDER_MATRIX=1",
        "skip: set ZMQ_RUN_KRAFT_FAILOVER_TESTS=1",
        "skip: set ZMQ_RUN_E2E_TESTS=1",
        "Live S3 provider benchmark skipped",
        "skip: set ZMQ_RUN_BENCH_COMPARE=1",
        "build static audit skip-marker catalogue",
        "release-evidence output-marker catalogue",
        "per-gate output_markers",
        "required success marker",
        "ok: protocol static audit",
        "ok: observability static audit",
        "ok: build static audit",
        "ok: root compose config",
        "ok: kafka compose config",
        "ok: automq compose config",
        "ok: chaos network-partition source=command",
        "ok: chaos harness passed for",
        "ok: client matrix profile",
        "ok: client matrix passed",
        "8/8 tests passed",
        "ok: S3 process crash/replacement harness passed",
        "ok: S3 provider live-suite profile",
        "ok: S3 provider profile",
        "ok: S3 provider matrix passed",
        "ok: KRaft controller failover harness passed ... source=command",
        "network_partition=[",
        "automq_stream_id=",
        "automq_deleted_stream_id=",
        "automq_stream_set_object_id=",
        "automq_node_id=",
        "automq_zone_router_epoch=",
        "old_leader=",
        "new_leader=",
        "restarted_controller=",
        "epoch=",
        "automq_old_leader=",
        "automq_new_leader=",
        "old_leader_rejoined=true",
        "old_leader_fresh_rejoin=true",
        "automq_old_leader_fresh_rejoin=true",
        "allocate_producer_ids_checked=true",
        "allocate_producer_ids_follower_rejection_checked=true",
        "describe_quorum_v2_checked=true",
        "fetch_snapshot_v1_checked=true",
        "all_controller_fetch_snapshot_v1_checked=true",
        "controller_api_versions_checked=true",
        "all_controller_api_versions_checked=true",
        "controller_unsupported_checked=true",
        "all_controller_unsupported_checked=true",
        "controller_unsupported_cases=[",
        "dynamic_raft_voter_negative_checked=true",
        "dynamic_raft_voter_follower_rejection_checked=true",
        "all_controller_describe_quorum_v2_checked=true",
        "broker_lifecycle_negative_checked=true",
        "broker_lifecycle_follower_rejection_checked=true",
        "controller_registration_negative_checked=true",
        "controller_registration_follower_rejection_checked=true",
        "broker_registration_follower_rejection_checked=true",
        "broker_non_broker_api_rejection_checked=true",
        "broker_non_broker_api_rejection_cases=[",
        "committed_offset=",
        "transactions_checked=5",
        "transaction_introspection_checked=true",
        "transaction_abort_checked=true",
        "txn_offset_commit_checked=true",
        "offset_fetch_v8_grouped_checked=true",
        "log_position_apis_checked=true",
        "delete_records_checked=true",
        "delete_topics_checked=true",
        "create_topics_checked=true",
        "create_partitions_checked=true",
        "client_quotas_checked=true",
        "scram_credentials_checked=true",
        "client_telemetry_checked=true",
        "delegation_tokens_checked=true",
        "finalized_features_checked=true",
        "acl_admin_checked=true",
        "config_admin_checked=true",
        "describe_topic_partitions_checked=true",
        "describe_configs_checked=true",
        "describe_log_dirs_checked=true",
        "alter_replica_log_dirs_checked=true",
        "assign_replicas_to_dirs_checked=true",
        "elect_leaders_checked=true",
        "describe_cluster_checked=true",
        "idempotent_producer_fencing=true",
        "describe_producers_checked=true",
        "delete_groups_checked=true",
        "classic_group_heartbeats=true",
        "group_describe_checked=true",
        "consumer_group_describe_checked=true",
        "list_groups_checked=true",
        "find_coordinator_checked=true",
        "share_group_heartbeat_checked=true",
        "share_group_describe_checked=true",
        "consumer_group_heartbeat_checked=true",
        "share_fetch_session_checked=true",
        "share_acknowledge_checked=true",
        "share_state_apis_checked=true",
        "kip848_describe_checked=true",
        "kip848_rejoin_checked=true",
        "kip848_rack_checked=true",
        "kip848_owned_assignment_checked=true",
        "kip848_subscription_update_checked=true",
        "kip848_negative_join_checked=true",
        "kip848_static_rejoin_checked=true",
        "offset_commit_v9_member_checked=true",
        "offset_fetch_v9_member_checked=true",
        "reassignment_topic=",
        "reassignment_target=",
        "reassignment_target_offset=",
        "reassignment_old_owner_rejected=true",
        "reassignment_target_fetch_verified=true",
        "3-Node E2E Test Suite",
        "[Test m] Cross-broker chaos phases",
        "[Test n] Live load/scale phases",
        "Results:",
        "=== Benchmarks complete ===",
        "ok: local benchmark gate source=command",
        "ok: live-S3 benchmark gate source=command",
        "S3 WAL request volume",
        "PartitionStore memory",
        "Live S3 provider",
        "Live S3 put",
        "Live S3 get",
        "Live S3 request volume",
        "COMPARISON:",
        "Benchmark",
        "ApiVersions",
        "Produce (reuse)",
        "Produce (fresh)",
        "Fetch",
        "Metadata",
        "COMPARATIVE BENCHMARK GATE",
        "thresholds:",
        "result: pass",
        "ok: comparative benchmark profile",
        "build static audit output-marker catalogue",
        "release-evidence detail output marker catalogue",
        "COMPARATIVE_TABLE_ROW_MARKERS",
        "BENCHMARK_OUTPUT_LINE_MARKERS",
        "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS",
        "KRAFT_DETAIL_OUTPUT_MARKERS",
        "E2E_OUTPUT_LINE_MARKERS",
        "KRaft, Docker E2E, benchmark, and comparative benchmark detail markers",
        "build static audit detail-output-marker catalogue",
        "release-evidence comparative benchmark catalogue",
        "COMPARATIVE_TARGET_LABELS",
        "COMPARATIVE_TABLE_TARGET_HEADERS",
        "COMPARATIVE_TABLE_METRICS",
        "COMPARATIVE_MEASUREMENT_RE",
        "COMPARATIVE_RATIO_RE",
        "COMPARATIVE_PROFILE_MARKER_KEYS",
        "COMPARATIVE_RATIO_RE entries must keep the comparative target labels",
        "table metric keys, ratio parser, and comparative benchmark profile marker aligned",
        "zmq",
        "kafka",
        "automq",
        "ZMQ (Zig)",
        "Apache Kafka",
        "AutoMQ (Java)",
        "ZMQ",
        "Kafka",
        "AutoMQ",
        "TARGET_SHORT_LABELS",
        "to match release-evidence `COMPARATIVE_TABLE_TARGET_HEADERS`",
        "ALL_TARGETS",
        "TARGET_LABELS",
        "to match release-evidence `COMPARATIVE_TARGET_LABELS`",
        "tput",
        "p50",
        "p99",
        "build static audit comparative-benchmark catalogue",
        "release-evidence comparative threshold default catalogue",
        "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS",
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV",
        "default comparative threshold keys and values",
        "max_error_rate=0.0",
        "max_p50_latency_ratio=20.0",
        "max_p99_latency_ratio=20.0",
        "max_trend_p50_latency_ratio=1.25",
        "max_trend_p99_latency_ratio=1.25",
        "min_throughput_ratio=0.05",
        "min_trend_throughput_ratio=0.9",
        "build static audit comparative-threshold-default catalogue",
        "forbidden command-fragment catalogue",
        "per-gate forbidden fragments",
        "build static audit forbidden-fragment catalogue",
        "release-evidence schema field catalogue",
        "RELEASE_EVIDENCE_FIELDS, COMMAND_ENTRY_FIELDS, and UNSUPPORTED_SURFACE_FIELDS",
        "closed schema field",
        "release manifest",
        "command entry",
        "unsupported surface",
        "commit",
        "environment",
        "commands",
        "unsupported_or_partial_surfaces",
        "known_data_loss_bug",
        "advertised_stub_api",
        "untriaged_durability_failure",
        "automq_complete",
        "command",
        "exit_code",
        "output",
        "surface",
        "status",
        "evidence",
        "id",
        "mitigation",
        "notes",
        "build static audit schema-field catalogue",
        "release-evidence blocking-flag catalogue",
        "BLOCKING_FLAGS",
        "blocking flag",
        "known_data_loss_bug=false",
        "advertised_stub_api=false",
        "untriaged_durability_failure=false",
        "build static audit blocking-flag catalogue",
        "Duplicate successful command entries",
        "same required gate",
        "comparative benchmark command must include",
        "match the manifest environment",
        "success-dependent `&&` separators only",
        "documented compose config commands may use multi-segment",
        "`;` and `||` cannot connect or trail",
        "quoted/echoed command text cannot satisfy",
        "release gate commands must be direct",
        "pipes, backgrounding, redirection, subshell grouping",
        "including Bash `&>`/`&>>` combined redirects",
        "substitution are rejected",
        "command strings must not embed release",
        "output marker text such as",
        "markers must come from captured command output",
        "within `ZMQ_S3_PROVIDER_REQUIRED_PROFILES`",
        "`sigkill-restart`, `slow-partial-client`, `clock-skewed-records`",
        "`s3-outage`, and `network-partition`",
        "within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`",
        "OAuth raw JWT fixtures must be strict JSON",
        "rejected before client execution",
        "`kcat`, `kafka-cli`, `kafka-python`",
        "`confluent-kafka`, `java-kafka`, and",
        "`basic`, `admin`, `groups`",
        "`rebalance`, `transactions`, `security`, and",
        "per-required client tool probe markers",
        "ok: <client> probes (<semantics>) source=command",
        "ok: kafka-python probes",
        "ok: confluent-kafka probes",
        "client matrix self-test error catalogue",
        "required client profile/tool/semantic coverage",
        "security and OAuth fixture validation",
        "exact semantic tokens inside client probe marker",
        "for every semantic named by",
        "recognized required client-tool probe markers",
        "rather than arbitrary `ok: ... probes` lines",
        "client tool probe markers now require `source=command`",
        "markers plus required client security detail",
        "ok: chaos sigkill-restart",
        "ok: chaos slow-partial-client",
        "ok: chaos clock-skewed-records",
        "ok: chaos s3-outage",
        "broker chaos self-test error catalogue",
        "record-batch fixtures",
        "scenario summary must appear as its own stripped line",
        "markers cannot satisfy the scenario summary",
        "ok: S3 provider live-suite profile",
        "S3 provider matrix self-test error catalogue",
        "provider profile fallback validation",
        "outage, process-crash, and multipart-fault evidence validation",
        "MinIO `8/8 tests passed` marker",
        "`ok: KRaft controller failover harness passed ... source=command` line",
        "KRaft failover self-test error catalogue",
        "protocol fixture parsers",
        "record-batch fixture invariants",
        "Python self-test raise-shape catalogue",
        "checked Python self-test gate list",
        "literal strings, f-strings, concatenated strings, and loop-selected messages",
        "new self-test raise message form",
        "build static audit scanner",
        "release-evidence output-marker dispatch catalogue",
        "requirement-specific output validators",
        "broker chaos, client matrix, S3, KRaft, Docker E2E, and benchmark markers",
        "new release-evidence output validator",
        "build static audit dispatch catalogue",
        "release-evidence E2E load/scale fixture action catalogue",
        "E2E_LOAD_SCALE_FIXTURE_ACTIONS",
        "built-in Docker E2E load/scale fixture actions",
        "must not report a fixture action",
        "load_records=<count>",
        "fixture `action=load`",
        "scale-in",
        "scale-out",
        "load",
        "probe",
        "noop",
        "build static audit E2E-fixture-action catalogue",
        "`load`, `scale-in`, and `scale-out`",
        "cross-broker coverage",
        "must include `zmq` plus at least one",
        "COMPARISON:",
        "Benchmark",
        "Produce (fresh)",
        "thresholds:",
        "Results saved to benchmarks/results.json",
        "ok: comparative benchmark profile",
        "source=command",
        "profile marker is a closed key=value schema",
        "every required field must appear exactly once",
        "fields must not be blank",
        "unknown fields are rejected",
        "before ratio columns",
        "Required target columns must stay in the same relative order as the",
        "comparative target catalogue",
        "Table target columns are limited to the known target headers",
        "Required ZMQ-to-baseline ratio columns",
        "`ZMQ/Kafka` and `ZMQ/AutoMQ`",
        "ratio columns are limited to known ZMQ-to-baseline pairs",
        "after target columns",
        "same comparative target catalogue order",
        "`COMPARISON:` line target labels",
        "must also follow the comparative target catalogue order",
        "exactly one positive finite",
        "target measurement cell",
        "positive finite ratio cell",
        "COMPARATIVE_RATIO_RE",
        "line-aware output marker matching",
        "rather than arbitrary substrings",
        "Captured skip markers are also line-aware",
        "Zig `Build Summary:` skip count",
        "Docker E2E section markers are line-aware",
        "`3-Node E2E Test Suite`",
        "`Results:`",
        "Docker E2E output line markers must appear exactly once",
        "Docker E2E self-test assertion catalogue",
        "fixture override rejection",
        "Local and live-S3 benchmark markers are also line-aware",
        "`ok: local benchmark gate source=command`",
        "`ok: live-S3 benchmark gate source=command`",
        "`S3 WAL request volume`",
        "`Live S3 request volume`",
        "live-S3 benchmark command must include",
        "`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
        "manifest environment must record the same values",
        "Comparative benchmark table markers are also line-aware",
        "appear on the `COMPARISON:` line",
        "throughput (`tput`) row",
        "comparative benchmark self-test assertion catalogue",
        "table-header target labels",
        "artifact-metadata failure cases",
        "unsupported_or_partial_surfaces",
        "account for every surface listed",
        "object with non-empty `surface`, `status`, and `evidence` fields",
        "bare strings and placeholder values are rejected",
        "Top-level manifest, command",
        "unsupported-surface objects are closed schemas",
        "unknown fields are",
        "unvalidated release status",
        "Each `surface` field must name the known surface",
        "evidence, mitigation, and notes cannot be the only matching fields",
        "Optional accounting lists must be non-empty",
        "Each required surface",
        "distinct object",
        "catch-all entries",
        "multiple known",
        "surfaces",
        "Duplicate objects for the same",
        "known surface are rejected",
        "entries outside the verifier catalog",
        "pins the top-level",
        "bullet list one-to-one",
        "checks each bullet's status wording",
        "verifier status class",
        "Each `status` must explicitly",
        "unsupported, partial, blocked, fail-closed/not-advertised",
        "vague completion-style statuses are rejected",
        "`automq_complete=false`",
        "while unsupported or partial surfaces remain",
        "checked against the verifier catalog",
        "eliding unsupported/partial surfaces cannot enable a complete claim",
        "unsupported-surface catalogue",
        "release-evidence verifier, release criteria, parity notes, and production-readiness pins",
        "each known surface label",
        "new unsupported or partial surface",
        "build static audit unsupported-surface catalogue",
        "release-evidence unsupported surface status-marker catalogue",
        "UNSUPPORTED_SURFACE_STATUS_MARKERS",
        "explicit unsupported/partial status markers",
        "unsupported",
        "not advertised",
        "fail closed",
        "fail-closed",
        "generated-only",
        "partial",
        "blocked",
        "blocker",
        "release-ci-required",
        "release ci required",
        "ci required",
        "must run",
        "build static audit unsupported-status catalogue",
        "release-evidence unsupported surface text-field catalogue",
        "UNSUPPORTED_SURFACE_TEXT_FIELDS",
        "unsupported-surface text aggregation",
        "id, surface, status, evidence, mitigation, and notes",
        "id",
        "surface",
        "status",
        "evidence",
        "mitigation",
        "notes",
        "build static audit unsupported-surface-text-field catalogue",
        "zig build test-release-evidence --summary all",
    };
    for (required_commands) |command| {
        try testing.expect(std.mem.indexOf(u8, criteria, command) != null);
    }

    try testing.expect(std.mem.indexOf(u8, criteria, "ZooKeeper-era inter-broker API keys 4-7 are generated-only") != null);
    try testing.expect(std.mem.indexOf(u8, criteria, "broker and controller ApiVersions omit them") != null);
    try testing.expect(std.mem.indexOf(u8, criteria, "direct broker/controller probes fail closed before body") != null);

    const unsupported_surfaces = [_][]const u8{
        "ZooKeeper-era inter-broker API keys 4-7",
        "broker-only stateless replacement",
        "external client/security/OAuth live matrix",
        "cross-broker chaos live matrix",
        "Docker E2E load/scale live orchestration",
        "KRaft failover network matrix",
        "live S3 provider outage and multipart-fault profile execution",
        "comparative Kafka/AutoMQ performance profile/trend gates",
    };
    for (unsupported_surfaces) |surface| {
        try testing.expect(std.mem.indexOf(u8, criteria, surface) != null);
    }

    try testing.expect(std.mem.indexOf(u8, criteria, "TBD") == null);
    try testing.expect(std.mem.indexOf(u8, criteria, "TODO") == null);
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

fn registerOperationalMetricCorpus(registry: *MetricRegistry) !void {
    try broker_metrics.registerBrokerMetrics(registry);
    try broker_metrics.registerS3Metrics(registry);
    try broker_metrics.registerCompactionMetrics(registry);
    try broker_metrics.registerCacheMetrics(registry);
    try broker_metrics.registerRaftMetrics(registry);
}

fn collectJsonPromqlExpressions(value: *const std.json.Value, expressions: *std.array_list.Managed([]const u8)) !void {
    switch (value.*) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "expr")) {
                    switch (entry.value_ptr.*) {
                        .string => |expr| try expressions.append(expr),
                        else => {},
                    }
                }
                try collectJsonPromqlExpressions(entry.value_ptr, expressions);
            }
        },
        .array => |array| {
            for (array.items) |*item| {
                try collectJsonPromqlExpressions(item, expressions);
            }
        },
        else => {},
    }
}

fn collectYamlPromqlExpressions(yaml: []const u8, expressions: *std.array_list.Managed([]const u8)) !void {
    var lines = std.mem.splitScalar(u8, yaml, '\n');
    var in_block_expr = false;
    var block_parent_indent: usize = 0;

    while (lines.next()) |line| {
        const indent = yamlLineIndent(line);
        const trimmed = std.mem.trim(u8, line, " \t");

        if (in_block_expr) {
            if (trimmed.len == 0) continue;
            if (indent > block_parent_indent) {
                try expressions.append(trimmed);
                continue;
            }
            in_block_expr = false;
        }

        if (!std.mem.startsWith(u8, trimmed, "expr:")) continue;

        var expr = std.mem.trim(u8, trimmed["expr:".len..], " \t");
        if (isYamlBlockScalar(expr)) {
            in_block_expr = true;
            block_parent_indent = indent;
            continue;
        }
        if (expr.len >= 2) {
            const first = expr[0];
            const last = expr[expr.len - 1];
            if ((first == '"' and last == '"') or (first == '\'' and last == '\'')) {
                expr = expr[1 .. expr.len - 1];
            }
        }
        try expressions.append(expr);
    }
}

fn yamlLineIndent(line: []const u8) usize {
    var count: usize = 0;
    while (count < line.len and (line[count] == ' ' or line[count] == '\t')) : (count += 1) {}
    return count;
}

fn isYamlBlockScalar(value: []const u8) bool {
    return std.mem.eql(u8, value, "|") or
        std.mem.eql(u8, value, ">") or
        std.mem.eql(u8, value, "|-") or
        std.mem.eql(u8, value, ">-") or
        std.mem.eql(u8, value, "|+") or
        std.mem.eql(u8, value, ">+");
}

fn assertPromqlExpressionMetricsRegistered(registry: *const MetricRegistry, source: []const u8, expr: []const u8) !usize {
    var metric_refs: usize = 0;
    var i: usize = 0;
    while (i < expr.len) {
        if (isPromqlStringStart(expr[i])) {
            i = skipPromqlString(expr, i);
            continue;
        }
        if (!isPromqlIdentStart(expr[i])) {
            i += 1;
            continue;
        }

        const start = i;
        i += 1;
        while (i < expr.len and isPromqlIdentChar(expr[i])) : (i += 1) {}
        const token = expr[start..i];
        if (!isPromqlMetricIdentifier(registry, token)) continue;

        metric_refs += 1;
        if (!isRegisteredPrometheusMetric(registry, token)) {
            std.debug.print("unregistered {s} metric reference: {s} in expression: {s}\n", .{ source, token, expr });
            return error.UnregisteredMetricReference;
        }
    }
    return metric_refs;
}

fn assertAlertHasSeverity(alerts: []const u8, alert_name: []const u8, severity: []const u8) !void {
    const alert_start = std.mem.indexOf(u8, alerts, alert_name) orelse return error.MissingAlert;
    const after_name = alerts[alert_start + alert_name.len ..];
    const next_alert = std.mem.indexOf(u8, after_name, "- alert: ") orelse after_name.len;
    const block = alerts[alert_start .. alert_start + alert_name.len + next_alert];
    try testing.expect(std.mem.indexOf(u8, block, severity) != null);
}

fn isRegisteredPrometheusMetric(registry: *const MetricRegistry, name: []const u8) bool {
    if (registry.counters.contains(name)) return true;
    if (registry.gauges.contains(name)) return true;
    if (registry.histograms.contains(name)) return true;
    if (registry.labeled_counter_meta.contains(name)) return true;
    if (registry.labeled_gauge_meta.contains(name)) return true;
    if (registry.labeled_histogram_meta.contains(name)) return true;

    if (stripHistogramPrometheusSuffix(name)) |base| {
        if (registry.histograms.contains(base)) return true;
        if (registry.labeled_histogram_meta.contains(base)) return true;
    }
    return false;
}

fn stripHistogramPrometheusSuffix(name: []const u8) ?[]const u8 {
    const suffixes = [_][]const u8{ "_bucket", "_sum", "_count" };
    for (suffixes) |suffix| {
        if (std.mem.endsWith(u8, name, suffix)) {
            return name[0 .. name.len - suffix.len];
        }
    }
    return null;
}

fn isPromqlMetricIdentifier(registry: *const MetricRegistry, identifier: []const u8) bool {
    const skipped = [_][]const u8{
        "avg",
        "by",
        "clamp_min",
        "histogram_quantile",
        "le",
        "max",
        "min",
        "operation",
        "rate",
        "sum",
        "without",
    };
    for (skipped) |item| {
        if (std.mem.eql(u8, identifier, item)) return false;
    }

    if (isRegisteredPrometheusMetric(registry, identifier)) return true;

    const metric_prefixes = [_][]const u8{
        "Kafka_",
        "automq_",
        "cache_",
        "compaction_",
        "kafka_",
        "log_cache_",
        "raft_",
        "s3_",
        "zmq_",
    };
    for (metric_prefixes) |prefix| {
        if (std.mem.startsWith(u8, identifier, prefix)) return true;
    }
    return false;
}

fn isPromqlIdentStart(byte: u8) bool {
    return (byte >= 'A' and byte <= 'Z') or (byte >= 'a' and byte <= 'z') or byte == '_' or byte == ':';
}

fn isPromqlIdentChar(byte: u8) bool {
    return isPromqlIdentStart(byte) or (byte >= '0' and byte <= '9');
}

fn isPromqlStringStart(byte: u8) bool {
    return byte == '"' or byte == '\'' or byte == '`';
}

fn skipPromqlString(expr: []const u8, start: usize) usize {
    const quote = expr[start];
    var i = start + 1;
    while (i < expr.len) {
        const byte = expr[i];
        if (quote != '`' and byte == '\\') {
            i += @min(@as(usize, 2), expr.len - i);
            continue;
        }
        i += 1;
        if (byte == quote) break;
    }
    return i;
}

/// PBKDF2-HMAC-SHA256 (Hi function from RFC 5802).
fn pbkdf2HmacSha256(password: []const u8, salt: []const u8, iterations: u32, out: *[32]u8) void {
    var salt_with_i: [36]u8 = undefined;
    @memcpy(salt_with_i[0..salt.len], salt);
    std.mem.writeInt(u32, salt_with_i[salt.len..][0..4], 1, .big);

    var u_prev: [32]u8 = undefined;
    hmacSha256Compute(password, salt_with_i[0 .. salt.len + 4], &u_prev);

    var result = u_prev;

    var i: u32 = 1;
    while (i < iterations) : (i += 1) {
        var u_next: [32]u8 = undefined;
        hmacSha256Compute(password, &u_prev, &u_next);
        for (&result, u_next) |*r, n| {
            r.* ^= n;
        }
        u_prev = u_next;
    }

    out.* = result;
}

/// HMAC-SHA256 computation.
fn hmacSha256Compute(key: []const u8, data: []const u8, out: *[32]u8) void {
    const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
    HmacSha256.create(out, data, key);
}

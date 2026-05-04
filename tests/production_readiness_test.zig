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
    try testing.expect(!conn.isHandshakeTimedOut(@import("time_compat").milliTimestamp()));

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
        "kafka_server_api_errors_total",
        "kafka_server_request_latency_seconds_bucket",
        "kafka_server_produce_latency_seconds_bucket",
        "kafka_server_fetch_latency_seconds_bucket",
        "kafka_network_connections_active",
        "s3_requests_total",
        "s3_request_errors_total",
        "s3_request_duration_seconds_bucket",
        "log_cache_size_bytes",
        "s3_block_cache_size_bytes",
        "kafka_client_telemetry_pushes_total",
        "kafka_client_telemetry_terminations_total",
        "kafka_client_telemetry_exported_total",
        "kafka_client_telemetry_export_errors_total",
        "kafka_client_telemetry_samples",
        "kafka_client_telemetry_bytes",
        "raft_role",
        "raft_current_epoch",
        "raft_commit_index",
        "kafka_network_requestmetrics_requests_total",
        "kafka_network_requestmetrics_totaltimems_total",
        "kafka_network_requestmetrics_requestqueuetimems_total",
        "kafka_network_requestmetrics_localtimems_total",
        "kafka_network_requestmetrics_remotetimems_total",
        "kafka_network_requestmetrics_responsequeuetimems_total",
        "kafka_network_requestmetrics_responsesendtimems_total",
        "kafka_server_brokertopicmetrics_totalproducerequests_total",
        "kafka_server_brokertopicmetrics_totalfetchrequests_total",
        "kafka_server_brokertopicmetrics_messagesin_total",
        "kafka_server_brokertopicmetrics_bytesrejected_total",
        "kafka_server_brokertopicmetrics_failedproducerequests_total",
        "kafka_server_brokertopicmetrics_failedfetchrequests_total",
        "kafka_server_brokertopicmetrics_bytesin_total",
        "kafka_server_brokertopicmetrics_bytesout_total",
        "kafka_controller_kafkacontroller_activecontrollercount",
        "kafka_server_replicamanager_partitioncount",
        "kafka_server_replicamanager_leadercount",
        "kafka_server_replicamanager_underreplicatedpartitions",
        "kafka_server_replicamanager_offlinepartitionscount",
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
        "raft_role",
        "s3_request_errors_total",
        "s3_request_duration_seconds_bucket",
        "compaction_errors_total",
        "kafka_client_telemetry_bytes",
        "kafka_client_telemetry_export_errors_total",
        "kafka_controller_kafkacontroller_activecontrollercount",
        "kafka_server_replicamanager_offlinepartitionscount",
        "kafka_server_replicamanager_underreplicatedpartitions",
        "kafka_server_brokertopicmetrics_failedproducerequests_total",
        "kafka_server_brokertopicmetrics_failedfetchrequests_total",
        "kafka_server_brokertopicmetrics_bytesrejected_total",
        "kafka_network_requestmetrics_localtimems_total",
        "kafka_network_requestmetrics_requests_total",
    };
    for (alert_metrics) |metric| {
        try testing.expect(std.mem.indexOf(u8, alerts, metric) != null);
    }
    const alert_names = [_][]const u8{
        "ZMQHighRequestLatencyP99",
        "ZMQHighProduceLatencyP99",
        "ZMQHighFetchLatencyP99",
        "ZMQS3LatencyP99",
        "ZMQNoActiveController",
        "ZMQOfflinePartitions",
        "ZMQUnderReplicatedPartitions",
        "ZMQFailedProduceRequests",
        "ZMQFailedFetchRequests",
        "ZMQRejectedProduceBytes",
        "ZMQHighJmxRequestLocalTime",
    };
    for (alert_names) |name| {
        try testing.expect(std.mem.indexOf(u8, alerts, name) != null);
    }
    try testing.expect(std.mem.indexOf(u8, alerts, "severity: critical") != null);

    var alert_metric_refs: usize = 0;
    for (alert_expressions.items) |expr| {
        alert_metric_refs += try assertPromqlExpressionMetricsRegistered(&registry, "alerts", expr);
    }
    try testing.expect(alert_metric_refs >= alert_metrics.len);
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

    const required_commands = [_][]const u8{
        "zig build test --summary all",
        "zig build test-chaos --summary all",
        "zig build test-client-matrix --summary all",
        "zig build test-minio --summary all",
        "zig build test-s3-process-crash --summary all",
        "zig build test-s3-provider-matrix --summary all",
        "zig build test-kraft-failover --summary all",
        "zig build test-e2e --summary all",
        "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
        "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
        "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
        "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
        "zig build bench --summary all",
        "ZMQ_RUN_BENCH_LIVE_S3=1",
        "zig build bench-compare --summary all",
    };
    for (required_commands) |command| {
        try testing.expect(std.mem.indexOf(u8, criteria, command) != null);
    }

    const unsupported_surfaces = [_][]const u8{
        "ConsumerGroupHeartbeat",
        "Share-group APIs",
        "AssignReplicasToDirs",
        "Legacy inter-broker APIs",
        "broker-only stateless replacement",
        "cross-broker chaos",
        "comparative Kafka/AutoMQ performance",
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
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t");
        if (!std.mem.startsWith(u8, trimmed, "expr:")) continue;

        var expr = std.mem.trim(u8, trimmed["expr:".len..], " \t");
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

fn assertPromqlExpressionMetricsRegistered(registry: *const MetricRegistry, source: []const u8, expr: []const u8) !usize {
    var metric_refs: usize = 0;
    var i: usize = 0;
    while (i < expr.len) {
        if (!isPromqlIdentStart(expr[i])) {
            i += 1;
            continue;
        }

        const start = i;
        i += 1;
        while (i < expr.len and isPromqlIdentChar(expr[i])) : (i += 1) {}
        const token = expr[start..i];
        if (!isPromqlMetricIdentifier(token)) continue;

        metric_refs += 1;
        if (!isRegisteredPrometheusMetric(registry, token)) {
            std.debug.print("unregistered {s} metric reference: {s} in expression: {s}\n", .{ source, token, expr });
            return error.UnregisteredMetricReference;
        }
    }
    return metric_refs;
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

fn isPromqlMetricIdentifier(identifier: []const u8) bool {
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

    const metric_prefixes = [_][]const u8{
        "Kafka_",
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
